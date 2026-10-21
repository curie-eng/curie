"""Conversational learning evaluation (#4780).

``curie dev learning-eval`` reaches this module through
``cli/scripts/learning-eval.sh``. ``run`` owns a private local stack driven only
through the pinned candidate CLI, scores ordinary conversations for durable
learning, and writes ``report.json`` and ``report.md``. The four ``verify-*``
commands recompute a recorded report offline (``verify-cleanup`` also reads the
caller's Docker daemon) and never trust a self-declared pass.

Everything except the public functions named in the plan is private.
"""

from __future__ import annotations

import argparse
import contextlib
import copy
import hashlib
import http.server
import json
import math
import os
import re
import secrets
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from fractions import Fraction
from pathlib import Path
from typing import Any

from .scorer import ScoreResult

# Fixed identities ---------------------------------------------------------------

REPORT_VERSION = 1
ACTOR_MODEL = "anthropic/claude-haiku-5.5"
JUDGE_MODEL = "anthropic/claude-sonnet-5.5"
PROVIDER_HOST = "openrouter.ai"
OPENROUTER_API = "https://openrouter.ai/api/v1"
MAX_BUDGET = Decimal("5.00")
# The pinned judge endpoint refuses disabled reasoning, so reasoning gets its
# own 512-token budget inside a 1536-token completion ceiling.
JUDGE_MAX_TOKENS = 1536
JUDGE_REASONING_TOKENS = 512
CASE_IDS = tuple(f"F{family}-P{phrase}" for family in range(1, 9) for phrase in range(1, 4))
CLASSES = ("skill", "memory-fact", "instruction", "schedule", "clarify", "nothing")
DURABLE = CLASSES[:4]
FAMILY_CLASS = {
    1: "skill",
    2: "instruction",
    3: "memory-fact",
    4: "schedule",
    5: "schedule",
    6: "clarify",
    7: "nothing",
    8: "skill",
}
DEFAULT_THRESHOLDS: dict[str, int | float] = {
    "precision": 0.9,
    "recall": 0.8,
    "falsewrite": 0.05,
    "correction_duplicates": 0,
    "heldout_control_improvement": 0.0,
    "scheduled_deliveries": 1,
}
CALIBRATION_GROUPS = ("good", "omitted", "obsolete", "duplicate", "fabricated")
PREDICATES = frozenset(
    {
        "active_count",
        "created_count",
        "updated_count",
        "deleted_count",
        "same_id",
        "revision_increased",
        "statement_equals",
        "body_contains_steps",
        "author_equals",
        "scope_equals",
        "channel_equals",
        "schedule_matches",
        "unchanged_other_stores",
    }
)
ACCOUNTING_BASES = frozenset(
    {"conservative-shared-key-upper-bound", "attributable-generation-cost"}
)
OWNER_LABEL = "curie.learning-eval.owner"
# Fixture placeholder channel IDs. Each scenario agent gets its own synthetic
# pair at render time, because a Slack channel route belongs to one agent.
CHANNELS = ("C0EXAMPLE1", "C0EXAMPLE2")
SYNTHETIC_CHANNEL = "C0SYNTHETIC"
EVAL_ISOLATE_PREFIX = "eval:"
THREAD_RE = re.compile(r"[0-9]+\.[0-9]{6}")
INSTANCE_RE = re.compile(r"(F[1-8]-P[1-3])-R([1-9][0-9]*)")
SHA_RE = re.compile(r"[0-9a-f]{64}")
COMMIT_RE = re.compile(r"[0-9a-f]{40}")
SOURCE_INPUTS = (
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
)
REQUIRED_SOURCE_INPUTS = (
    "cli/scripts/learning-eval.sh",
    "apps/worker/src/curie_worker/eval/learning.py",
)
FIXTURES = Path(__file__).resolve().parent / "fixtures"
SOURCE_ROOT = Path(__file__).resolve().parents[5]
MAX_STDERR = 400


class _Invalid(ValueError):
    """A report or input that fails its contract."""


# Strict JSON and small helpers ---------------------------------------------------


def _sha(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _json_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()


def _no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise _Invalid(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _reject_constant(name: str) -> Any:
    raise _Invalid(f"non-finite JSON number {name}")


def _strict_loads(data: bytes) -> Any:
    text = data.decode("utf-8", errors="strict")
    return json.loads(text, object_pairs_hook=_no_duplicates, parse_constant=_reject_constant)


def _read_json(path: Path) -> Any:
    with path.open("rb") as handle:
        data = handle.read()
    return _strict_loads(data)


def _is_int(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_str(value: object) -> bool:
    return isinstance(value, str) and bool(value)


def _obj(value: object, label: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise _Invalid(f"{label} must be an object")
    return value


def _lst(value: object, label: str) -> list[Any]:
    if not isinstance(value, list):
        raise _Invalid(f"{label} must be an array")
    return value


def _text(value: object, label: str) -> str:
    if not _is_str(value):
        raise _Invalid(f"{label} must be a nonempty string")
    assert isinstance(value, str)
    return value


def _int(value: object, label: str, minimum: int | None = None) -> int:
    if not _is_int(value):
        raise _Invalid(f"{label} must be an integer")
    assert isinstance(value, int)
    if minimum is not None and value < minimum:
        raise _Invalid(f"{label} must be at least {minimum}")
    return value


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise _Invalid(message)


def _decimal(value: object, label: str) -> Decimal:
    if not isinstance(value, str):
        raise _Invalid(f"{label} must be a decimal string")
    try:
        amount = Decimal(value)
    except InvalidOperation as exc:
        raise _Invalid(f"{label} is not a decimal") from exc
    if not amount.is_finite() or amount < 0:
        raise _Invalid(f"{label} must be finite and nonnegative")
    return amount


def _ratio(value: object, label: str) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise _Invalid(f"{label} must be a number")
    number = float(value)
    if not math.isfinite(number) or not 0 <= number <= 1:
        raise _Invalid(f"{label} must be finite within [0, 1]")
    return number


def _utc(value: object, label: str) -> datetime:
    text = _text(value, label)
    if not text.endswith("Z"):
        raise _Invalid(f"{label} must be a UTC RFC3339 timestamp")
    try:
        moment = datetime.fromisoformat(text[:-1] + "+00:00")
    except ValueError as exc:
        raise _Invalid(f"{label} must be a UTC RFC3339 timestamp") from exc
    return moment


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _tail(text: str, lines: int = 40, limit: int = 6000) -> str:
    """The last lines of a child's output: a failure's cause is at the end."""

    kept = "\n".join(text.rstrip().splitlines()[-lines:])
    return kept if len(kept) <= limit else "..." + kept[-limit:]


def _excerpt(text: str, limit: int = 200) -> str:
    flat = " ".join(str(text).split())
    return json.dumps(flat if len(flat) <= limit else flat[:limit] + "...")


def _bounded(text: str) -> str:
    return text if len(text) <= MAX_STDERR else text[:MAX_STDERR] + "..."


# Fixture loaders ------------------------------------------------------------------


def _check_predicate(raw: object, label: str) -> None:
    predicate = _obj(raw, label)
    name = predicate.get("name")
    _require(name in PREDICATES, f"{label} has unsupported predicate {name!r}")
    _require(predicate.get("class") in DURABLE, f"{label} has an invalid class")
    _text(predicate.get("target"), f"{label} target")
    argument = "steps" if name == "body_contains_steps" else "value"
    _require(
        set(predicate) == {"name", "class", "target", argument},
        f"{label} must carry exactly name, class, target and {argument}",
    )
    value = predicate[argument]
    if name in {"active_count", "created_count", "updated_count", "deleted_count"}:
        _int(value, f"{label} value", 0)
    elif name in {"same_id", "revision_increased", "unchanged_other_stores"}:
        _require(value is True, f"{label} value must be true")
    elif name == "body_contains_steps":
        steps = _lst(value, f"{label} steps")
        _require(bool(steps) and all(_is_str(step) for step in steps), f"{label} steps")
    elif name == "schedule_matches":
        _require(isinstance(value, dict) and bool(value), f"{label} value must be an object")
    elif name == "channel_equals":
        _require(_is_str(value) or isinstance(value, dict), f"{label} value")
    else:
        _require(_is_str(value), f"{label} value must be a nonempty string")


def _load_corpus(path: Path) -> tuple[dict[str, Any], tuple[dict[str, object], ...]]:
    document = _obj(_read_json(path), "case corpus")
    _require(document.get("version") == 1 and _is_int(document.get("version")), "case version")
    guidance = _obj(document.get("guidance"), "guidance")
    _text(guidance.get("text"), "guidance text")
    families = _lst(document.get("families"), "families")
    _require(len(families) == 8, "the corpus must have exactly eight families")
    rows: list[dict[str, object]] = []
    seen: set[str] = set()
    for index, raw_family in enumerate(families, 1):
        family = _obj(raw_family, "family")
        _require(family.get("id") == f"F{index}", f"family {index} has the wrong identity")
        scenarios = _lst(family.get("scenarios"), "scenarios")
        _require(len(scenarios) == 3, f"family F{index} needs exactly three paraphrases")
        for phrase, raw in enumerate(scenarios, 1):
            scenario = _obj(raw, "scenario")
            identity = scenario.get("id")
            _require(identity == f"F{index}-P{phrase}", f"unexpected scenario {identity!r}")
            _require(identity not in seen, f"duplicate scenario {identity!r}")
            seen.add(str(identity))
            messages = _lst(scenario.get("messages"), f"{identity} messages")
            _require(bool(messages) and all(_is_str(m) for m in messages), f"{identity} messages")
            requires = _lst(scenario.get("requires"), f"{identity} requires")
            _require(all(item in DURABLE for item in requires), f"{identity} requires")
            decisions = _lst(scenario.get("decisions"), f"{identity} decisions")
            _require(bool(decisions), f"{identity} needs a scored decision")
            decision_ids: set[str] = set()
            for raw_decision in decisions:
                decision = _obj(raw_decision, f"{identity} decision")
                decision_id = _text(decision.get("id"), f"{identity} decision id")
                _require(decision_id not in decision_ids, f"{identity} repeats {decision_id}")
                decision_ids.add(decision_id)
                message = _int(decision.get("message"), f"{identity} message index", 0)
                _require(message < len(messages), f"{identity} message index out of range")
                _require(decision.get("expected") in CLASSES, f"{identity} expected class")
                predicates = _lst(decision.get("predicates"), f"{identity} predicates")
                _require(bool(predicates), f"{identity} needs predicates")
                for raw_predicate in predicates:
                    _check_predicate(raw_predicate, f"{identity} predicate")
            rows.append(copy.deepcopy(scenario))
    return copy.deepcopy(guidance), tuple(rows)


def load_cases(path: Path) -> tuple[dict[str, object], ...]:
    """Load and validate the 24-scenario learning corpus."""

    return _load_corpus(path)[1]


def _load_calibration_document(path: Path) -> dict[str, Any]:
    document = _obj(_read_json(path), "calibration")
    _require(document.get("version") == 1 and _is_int(document.get("version")), "version")
    examples = _lst(document.get("examples"), "examples")
    _require(len(examples) >= 10, "calibration needs at least ten examples")
    seen: set[str] = set()
    for raw in examples:
        example = _obj(raw, "calibration example")
        identity = _text(example.get("id"), "example id")
        _require(identity not in seen, f"duplicate calibration example {identity}")
        seen.add(identity)
        _require(example.get("group") in CALIBRATION_GROUPS, f"{identity} group")
        _require(example.get("expected") in ("PASS", "FAIL"), f"{identity} expected")
        _require(example.get("action") in (*CLASSES, None), f"{identity} action")
        _text(example.get("rubric"), f"{identity} rubric")
        deterministic = _obj(example.get("deterministic"), f"{identity} deterministic")
        _require(isinstance(deterministic.get("passed"), bool), f"{identity} deterministic")
        evidence = _obj(example.get("evidence"), f"{identity} evidence")
        _require(isinstance(evidence.get("output"), str), f"{identity} evidence output")
        trajectory = _lst(evidence.get("trajectory"), f"{identity} trajectory")
        _require(all(isinstance(item, str) for item in trajectory), f"{identity} trajectory")
        refs = _obj(evidence.get("refs"), f"{identity} refs")
        _require(bool(refs) and all(_is_str(key) for key in refs), f"{identity} refs")
    groups = {str(_obj(raw, "example")["group"]) for raw in examples}
    _require(groups == set(CALIBRATION_GROUPS), "calibration must cover all five groups")
    for group in CALIBRATION_GROUPS:
        count = sum(_obj(raw, "example")["group"] == group for raw in examples)
        _require(count >= 2, f"calibration group {group} needs two examples")
    controls = _lst(document.get("deterministic_controls", []), "deterministic controls")
    for raw in controls:
        control = _obj(raw, "deterministic control")
        _require(control.get("group") in CALIBRATION_GROUPS, "control group")
        _text(control.get("id"), "control id")
    return copy.deepcopy(document)


def load_calibration(path: Path) -> tuple[dict[str, object], ...]:
    """Load and validate the judge calibration examples."""

    document = _load_calibration_document(path)
    return tuple(dict(example) for example in document["examples"])


def _file_sha(path: Path) -> str:
    with path.open("rb") as handle:
        return _sha(handle.read())


def _bundle_sha(bundle: Path) -> str:
    rows = [
        {"path": str(path.relative_to(bundle)), "sha256": _file_sha(path)}
        for path in sorted(bundle.rglob("*"))
        if path.is_file()
    ]
    return _sha(_json_bytes(rows))


def _source_manifest(root: Path) -> list[dict[str, str]]:
    paths = list(SOURCE_INPUTS)
    paths += [str(path.relative_to(root)) for path in FIXTURES.rglob("*") if path.is_file()]
    return [{"path": path, "sha256": _file_sha(root / path)} for path in sorted(set(paths))]


# Report validation ------------------------------------------------------------------


def _summary(rows: Sequence[Mapping[str, Any]], thresholds: Mapping[str, Any]) -> dict[str, Any]:
    """Classification, distractor false-write and duplicate gates for one population."""

    metrics: dict[str, Any] = {}
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
    # Only the F8 "correct" decision measures correction duplicates; seed rows never count.
    corrections = [
        row
        for row in eligible
        if str(row["fixture_id"]).startswith("F8-") and row["decision_id"] == "correct"
    ]
    duplicates = sum(row["correction_duplicates"] for row in corrections)
    gates: dict[str, Any] = {}
    for name in ("precision", "recall"):
        values = [row[name] for row in metrics.values()]
        if excluded:
            gates[name] = {"status": "NOT_AVAILABLE", "value": None}
        elif any(value is None for value in values):
            gates[name] = {"status": "FAIL", "value": None}
        else:
            value = min(values)
            passed = Decimal(str(value)) >= Decimal(str(thresholds[name]))
            gates[name] = {"status": "PASS" if passed else "FAIL", "value": value}
    falsewrite_pass = falsewrite_rate is not None and Decimal(str(falsewrite_rate)) <= Decimal(
        str(thresholds["falsewrite"])
    )
    gates["falsewrite"] = {
        "status": "PASS" if falsewrite_pass else "FAIL",
        "value": falsewrite_rate,
    }
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
    attempted = (
        sum(bool(row["attempts"]) for row in distractors) / len(distractors)
        if distractors
        else None
    )
    return {
        "per_class": metrics,
        "gates": gates,
        "confusion": confusion,
        "distractor": {
            "evaluated": len(distractors),
            "falsewrite_count": falsewrites,
            "falsewrite_rate": falsewrite_rate,
            "attempted_write_rate": attempted,
        },
        "unavailable_off_target_writes": off_target,
    }


def _check_thresholds(value: object) -> dict[str, Any]:
    thresholds = _obj(value, "thresholds")
    _require(set(thresholds) == set(DEFAULT_THRESHOLDS), "thresholds have the wrong fields")
    for name in ("precision", "recall", "falsewrite", "heldout_control_improvement"):
        _ratio(thresholds[name], f"threshold {name}")
    _int(thresholds["correction_duplicates"], "duplicate threshold", 0)
    _int(thresholds["scheduled_deliveries"], "scheduled delivery threshold", 1)
    return thresholds


def _is_custom(thresholds: Mapping[str, Any]) -> bool:
    return any(thresholds[name] != value for name, value in DEFAULT_THRESHOLDS.items())


def _state_items(state: object, label: str) -> dict[tuple[str, str], Any]:
    """Durable rows of one recorded state keyed by (class, immutable id)."""

    stores = _obj(state, label)
    _require(set(stores) == {"skills", "facts", "schedules"}, f"{label} stores")
    items: dict[tuple[str, str], Any] = {}
    for store, rows in stores.items():
        for raw in _lst(rows, f"{label} {store}"):
            row = _obj(raw, f"{label} row")
            identity = _text(row.get("id"), f"{label} row id")
            if store == "skills":
                kind = "skill"
            elif store == "schedules":
                kind = "schedule"
            else:
                kind = "instruction" if row.get("kind") == "instruction" else "memory-fact"
            _require((kind, identity) not in items, f"{label} repeats {identity}")
            items[(kind, identity)] = row
    return items


def _state_diff(before: object, after: object) -> set[tuple[str, str]]:
    old = _state_items(before, "before state")
    new = _state_items(after, "after state")
    changed = {key for key in old.keys() ^ new.keys()}
    changed |= {key for key in old.keys() & new.keys() if old[key] != new[key]}
    return changed


@dataclass
class _Report:
    path: Path
    data: dict[str, Any]
    binary: dict[str, Any] = field(default_factory=dict)
    evidence: dict[str, Any] = field(default_factory=dict)
    invocations: dict[str, dict[str, Any]] = field(default_factory=dict)
    order: dict[str, int] = field(default_factory=dict)
    cases: dict[tuple[int, str], dict[str, Any]] = field(default_factory=dict)
    corpus: dict[str, dict[str, Any]] = field(default_factory=dict)
    guidance_text: str = ""
    repetitions: int = 0
    thresholds: dict[str, Any] = field(default_factory=dict)

    def value(self, ref: object, kind: str | None = None) -> dict[str, Any]:
        _require(isinstance(ref, str) and ref in self.evidence, f"unresolved evidence {ref!r}")
        record = self.evidence[str(ref)]
        if kind is not None:
            _require(record["kind"] == kind, f"evidence {ref} is not {kind}")
        return _obj(record["value"], f"evidence {ref}")

    def invocation(self, ref: object) -> dict[str, Any]:
        _require(isinstance(ref, str) and ref in self.invocations, f"unknown invocation {ref!r}")
        return self.invocations[str(ref)]

    def scope_case(self, row: Mapping[str, Any], prefix: str | None = None) -> dict[str, Any]:
        repetition = _int(row.get("repetition"), "repetition", 1)
        case_id = _text(row.get("case_id"), "case_id")
        _text(row.get("agent_id"), "agent_id")
        case = self.cases.get((repetition, case_id))
        _require(case is not None, f"{case_id} is not an executed case of repetition {repetition}")
        assert case is not None
        _require(case["agent_id"] == row["agent_id"], f"{case_id} is bound to another agent")
        if prefix is not None:
            _require(case["fixture_id"].startswith(prefix), f"{case_id} is not a {prefix} case")
        return case


def _same_scope(record: Mapping[str, Any], row: Mapping[str, Any], label: str) -> None:
    for key in ("repetition", "case_id", "agent_id"):
        _require(record.get(key) == row.get(key), f"{label} belongs to another scope")


def _load_report(path: Path) -> _Report:
    data = _obj(_read_json(path), "report")
    _require(_is_int(data.get("report_version")), "report_version must be an integer")
    _require(data["report_version"] == REPORT_VERSION, "unsupported report_version")
    report = _Report(path=path, data=data)
    report.repetitions = _int(data.get("repetitions"), "repetitions", 1)
    _text(data.get("seed"), "seed")
    _text(data.get("output_dir"), "output_dir")
    report.thresholds = _check_thresholds(data.get("thresholds"))
    _require(data.get("custom_thresholds") is _is_custom(report.thresholds), "custom label")
    _check_identity(report)
    _check_fixtures(report)
    raw_evidence = _obj(data.get("evidence"), "evidence")
    for key, raw in raw_evidence.items():
        record = _obj(raw, f"evidence {key}")
        _require(set(record) == {"kind", "sha256", "value"}, f"evidence {key} fields")
        _text(record["kind"], f"evidence {key} kind")
        _require(record["sha256"] == _sha(_json_bytes(record["value"])), f"evidence {key} digest")
    report.evidence = raw_evidence
    for index, raw in enumerate(_lst(data.get("invocations"), "invocations")):
        invocation = _obj(raw, "invocation")
        identity = _text(invocation.get("invocation_id"), "invocation id")
        _require(identity not in report.invocations, f"duplicate invocation {identity}")
        argv = _lst(invocation.get("argv"), f"{identity} argv")
        _require(bool(argv) and all(isinstance(arg, str) for arg in argv), f"{identity} argv")
        _require(argv[0] == report.binary["path"], f"{identity} used another executable")
        _require(invocation.get("binary") == report.binary, f"{identity} binary identity")
        _require(invocation.get("source_root") == data["source_root"], f"{identity} source")
        _require(
            invocation.get("source_tree_sha256") == data["source_tree_sha256"],
            f"{identity} source tree",
        )
        _int(invocation.get("exit_code"), f"{identity} exit code")
        _text(invocation.get("cwd"), f"{identity} cwd")
        _int(invocation.get("repetition"), f"{identity} repetition", 0)
        report.value(invocation.get("result_ref"))
        report.invocations[identity] = invocation
        report.order[identity] = index
    return report


def _version_of(path: str) -> str:
    completed = subprocess.run(
        [path, "--version"],
        capture_output=True,
        timeout=60,
        check=False,
        stdin=subprocess.DEVNULL,
    )
    if completed.returncode != 0:
        raise _Invalid("the recorded executable did not report its version")
    return completed.stdout.decode("utf-8", errors="strict").strip()


def _check_identity(report: _Report) -> None:
    data = report.data
    binary = _obj(data.get("binary"), "binary")
    _require(set(binary) == {"path", "sha256", "version"}, "binary identity fields")
    path = Path(_text(binary["path"], "binary path"))
    _require(path.is_absolute() and path.is_file(), "the recorded executable is missing")
    _require(_file_sha(path) == binary["sha256"], "the recorded executable bytes changed")
    _require(_version_of(str(path)) == binary["version"], "the recorded executable version")
    report.binary = binary
    root = Path(_text(data.get("source_root"), "source_root"))
    manifest = _lst(data.get("source_manifest"), "source_manifest")
    paths: list[str] = []
    for raw in manifest:
        entry = _obj(raw, "source manifest entry")
        _require(set(entry) == {"path", "sha256"}, "source manifest entry fields")
        relative = _text(entry["path"], "source path")
        _require(not Path(relative).is_absolute() and ".." not in Path(relative).parts, "path")
        _require(_file_sha(root / relative) == entry["sha256"], f"source input {relative} changed")
        paths.append(relative)
    _require(paths == sorted(set(paths)), "source manifest must be sorted and unique")
    _require(all(item in paths for item in REQUIRED_SOURCE_INPUTS), "source manifest coverage")
    source_sha = _sha(_json_bytes(manifest))
    _require(data.get("source_tree_sha256") == source_sha, "source tree digest")
    build = _obj(data.get("build_association"), "build_association")
    _require(set(build) == {"candidate_commit", "input_manifest_sha256", "binary_sha256"}, "build")
    _require(isinstance(build["candidate_commit"], str), "candidate commit")
    _require(bool(COMMIT_RE.fullmatch(build["candidate_commit"])), "candidate commit")
    _require(build["input_manifest_sha256"] == source_sha, "build input manifest")
    _require(build["binary_sha256"] == binary["sha256"], "build binary digest")


def _check_fixtures(report: _Report) -> None:
    data = report.data
    fixtures = _obj(data.get("fixtures"), "fixtures")
    _require(set(fixtures) == {"learning-cases.json", "learning-calibration.json"}, "fixture set")
    for name, raw in fixtures.items():
        entry = _obj(raw, f"fixture {name}")
        _require(
            _file_sha(Path(_text(entry.get("path"), "fixture path"))) == entry.get("sha256"),
            f"fixture {name} digest",
        )
    cases_path = Path(fixtures["learning-cases.json"]["path"])
    guidance, rows = _load_corpus(cases_path)
    report.guidance_text = str(guidance["text"])
    report.corpus = {str(row["id"]): dict(row) for row in rows}
    _require(data.get("guidance_sha256") == _sha(report.guidance_text.encode()), "guidance digest")
    if "applied_guidance_sha256" in data:
        _require(
            data["applied_guidance_sha256"]
            == _sha(_applied_guidance(report.guidance_text).encode()),
            "applied guidance digest",
        )
    _require(
        data.get("bundle_sha256") == _bundle_sha(cases_path.parent / "learning-bundle"),
        "bundle digest",
    )
    _load_calibration_document(Path(fixtures["learning-calibration.json"]["path"]))


# ADR 0239 memory list rows: every field the twin-memory schema requires.
MEMORY_ROW_FIELDS = frozenset(
    {
        "id",
        "scope",
        "channel",
        "kind",
        "title",
        "statement",
        "author",
        "stated_at",
        "conversation",
        "archived",
        "version",
    }
)
# Appended at run time to the pinned operator guidance. Operator guidance replaces
# the learning default verbatim (ADR 0239), so the permission must be stated here.
GUIDANCE_INSTRUCTION_SENTENCE = (
    "When someone asks for a lasting convention for how replies should look or end, "
    "save it as a standing instruction, not as a fact."
)


def _applied_guidance(text: str) -> str:
    return f"{text} {GUIDANCE_INSTRUCTION_SENTENCE}"


def _kind_enum(schema: Mapping[str, Any]) -> list[Any] | None:
    """The memory fact `kind` enum, resolving one local `#/$defs/...` reference.

    None when the fact definition declares no kind; a dangling or non-local
    reference, or a kind without an enum, is an error."""

    defs = _obj(schema.get("$defs"), "$defs")
    fact = _obj(defs.get("fact"), "fact definition")
    properties = _obj(fact.get("properties"), "fact properties")
    if "kind" not in properties:
        return None
    kind = _obj(properties["kind"], "kind declaration")
    ref = kind.get("$ref")
    if ref is not None:
        prefix = "#/$defs/"
        if not isinstance(ref, str) or not ref.startswith(prefix):
            raise _Invalid(f"the memory kind reference {ref!r} is not a local definition")
        target = defs.get(ref[len(prefix) :])
        if not isinstance(target, dict):
            raise _Invalid(f"the memory kind reference {ref} is dangling")
        kind = target
    enum = kind.get("enum")
    if not isinstance(enum, list):
        raise _Invalid("the embedded memory schema declares an ambiguous kind")
    return enum


def _capabilities(report: _Report) -> dict[str, bool]:
    raw = _obj(report.data.get("capabilities"), "capabilities")
    _require(set(raw) == set(DURABLE), "capabilities must name the four durable classes")
    result: dict[str, bool] = {}
    for kind, value in raw.items():
        entry = _obj(value, f"capability {kind}")
        available: Any = entry.get("available")
        _require(isinstance(available, bool), f"capability {kind} availability")
        evidence = report.value(entry.get("evidence_ref"))
        record_kind = report.evidence[entry["evidence_ref"]]["kind"]
        if available:
            _require(record_kind == "discovery", f"capability {kind} needs discovery evidence")
        else:
            _require(record_kind == "capability-absence", f"{kind} absence needs evidence")
            _require(evidence.get("capability") == kind, f"{kind} absence evidence")
            _require(
                evidence.get("exit_code") == 0 and _is_int(evidence.get("exit_code")),
                f"{kind} absence must come from a successful read",
            )
            _require(evidence.get("binary") == report.binary, f"{kind} absence binary")
        result[kind] = available
    _require(result["memory-fact"], "memory must be observable; a failed read is an error")
    schema = _obj(report.data.get("memory_schema"), "memory_schema")
    discovery = report.value(schema.get("evidence_ref"), "discovery")
    embedded = _obj(discovery.get("memory_schema"), "embedded memory schema")
    _require(schema.get("sha256") == _sha(_json_bytes(embedded)), "memory schema digest")
    _require(schema.get("id") == embedded.get("$id"), "memory schema id")
    if result["instruction"]:
        _require("instruction" in (_kind_enum(embedded) or []), "instruction kind")
    return result


def _check_cases(report: _Report) -> None:
    data = report.data
    collector = _obj(data.get("collector"), "collector")
    agents: set[str] = set()
    for raw in _lst(data.get("cases"), "cases"):
        case = _obj(raw, "case")
        repetition = _int(case.get("repetition"), "case repetition", 1)
        _require(repetition <= report.repetitions, "case repetition beyond N")
        fixture = _text(case.get("fixture_id"), "fixture_id")
        _require(fixture in CASE_IDS, f"unknown fixture {fixture}")
        _require(case.get("case_id") == f"{fixture}-R{repetition}", "case instance identity")
        agent = _text(case.get("agent_id"), "case agent")
        _require(agent not in agents, f"agent {agent} is reused across scenarios")
        agents.add(agent)
        _require(case.get("primary") is True, "case must be a primary input")
        key = (repetition, str(case["case_id"]))
        _require(key not in report.cases, f"duplicate case {key}")
        invocation = report.invocation(case.get("invocation_id"))
        _same_scope(invocation, case, "primary turn")
        argv = invocation["argv"]
        _require("message" in argv and "--thread" in argv, "primary turn must be local message")
        thread = argv[argv.index("--thread") + 1]
        _require(bool(THREAD_RE.fullmatch(thread)), "primary thread must be timestamp shaped")
        _require(invocation["cwd"] == data["output_dir"], "message cwd must be the output dir")
        _require(invocation["exit_code"] == 0, "primary turn failed")
        _require(case.get("reply_ref") == invocation["result_ref"], "reply binding")
        reply = _obj(report.value(case.get("reply_ref")).get("result"), "reply result")
        _require(reply.get("finalized") is True, f"{case['case_id']} did not finalize")
        observation = report.value(case.get("transcript_ref"))
        _require("transcript" in observation, f"{case['case_id']} transcript is missing")
        _same_scope(observation, case, "transcript")
        if case.get("actor_generation_id") is None:
            _text(case.get("actor_record_id"), "actor record")
        else:
            _text(case.get("actor_generation_id"), "actor generation")
        _require(case.get("stub_port") == collector.get("port_b"), "message stub port")
        _require(case.get("stub_bind_succeeded") is True, "message stub bind")
        case["_thread"] = thread
        report.cases[key] = case
    for repetition in range(1, report.repetitions + 1):
        present = {
            fixture
            for (rep, _), fixture in ((k, v["fixture_id"]) for k, v in report.cases.items())
            if rep == repetition
        }
        _require(present == set(CASE_IDS), f"repetition {repetition} lacks primary scenarios")


CONSENT_DECISIONS = ("consent-offer", "consent-refusal", "consent-approval")
NONCE_RE = re.compile(r"[a-z]+-[0-9a-f]{6}")


def _fixture_decisions(fixture: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The fixture's labeled decisions, plus the F1 consent control's three messages
    as first-class decisions (offer, refusal, approval). The consent control has no
    rubric of its own, so its decisions are judged with the case's first rubric."""

    specs = [dict(d) for d in fixture["decisions"]]
    consent = next(
        (
            c
            for c in fixture.get("controls", [])
            if isinstance(c, dict) and c.get("id") == "consent"
        ),
        None,
    )
    if consent is None:
        return specs
    subject = str(consent.get("subject") or fixture.get("subject"))
    forbids = consent.get("forbid_persistence_attempts") or [False] * len(CONSENT_DECISIONS)
    for index, (decision_id, text, expected, forbid) in enumerate(
        zip(CONSENT_DECISIONS, consent["messages"], consent["expected"], forbids, strict=True)
    ):
        unchanged = [
            {"name": "unchanged_other_stores", "class": kind, "target": "*", "value": True}
            for kind in DURABLE
            if kind != expected
        ]
        writes = (
            [
                {"name": "active_count", "class": expected, "target": subject, "value": 1},
                {"name": "created_count", "class": expected, "target": subject, "value": 1},
            ]
            if expected in DURABLE
            else []
        )
        specs.append(
            {
                "id": decision_id,
                "consent_message": index,
                "text": text,
                "expected": expected,
                "predicates": writes + unchanged,
                "rubric": fixture["decisions"][0]["rubric"],
                "forbid_persistence_attempts": forbid is True,
            }
        )
    return specs


def _decision_rubric(spec: Mapping[str, Any]) -> str:
    return str(spec.get("rubric") or "Judge the learning outcome.")


def _decision_request(fixture: Mapping[str, Any], spec: Mapping[str, Any]) -> str:
    """The rendered user message a decision scores."""

    if "text" in spec:
        return str(spec["text"])
    return str(fixture["messages"][int(spec["message"])])


def _expected_meaning(spec: Mapping[str, Any]) -> str:
    """What the decision must mean: its expected class, the rendered fixture
    predicates (which carry the taught steps, values and targets) and the rubric."""

    return (
        f"Expected class: {spec['expected']}. "
        f"Expected stored outcome: {json.dumps(spec['predicates'], sort_keys=True)}. "
        f"Rubric: {_decision_rubric(spec)}"
    )


def _required_decisions(
    report: _Report, capabilities: Mapping[str, bool]
) -> set[tuple[int, str, str]]:
    """Every fixture-derived decision of every executed case. Consent decisions are
    executed only when every capability the fixture requires is available."""

    required: set[tuple[int, str, str]] = set()
    for (repetition, case_id), case in report.cases.items():
        fixture = report.corpus[case["fixture_id"]]
        runnable = all(capabilities.get(str(k), False) for k in fixture.get("requires", []))
        for spec in _fixture_decisions(fixture):
            if "consent_message" in spec and not runnable:
                continue
            required.add((repetition, case_id, str(spec["id"])))
    return required


def _check_decisions(report: _Report, capabilities: Mapping[str, bool]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    seen: set[tuple[int, str, str]] = set()
    covered: set[tuple[int, str]] = set()
    for raw in _lst(report.data.get("decisions"), "decisions"):
        row = _obj(raw, "decision")
        case_id = _text(row.get("case_id"), "decision case_id")
        match = INSTANCE_RE.fullmatch(case_id)
        _require(match is not None, f"decision instance {case_id!r}")
        assert match is not None
        _require(row.get("repetition") == int(match.group(2)), "decision repetition suffix")
        case = report.scope_case(row)
        _require(row.get("fixture_id") == case["fixture_id"], "decision fixture")
        decision_id = _text(row.get("decision_id"), "decision id")
        identity = (int(row["repetition"]), case_id, decision_id)
        _require(identity not in seen, f"duplicate decision {identity}")
        seen.add(identity)
        covered.add((int(row["repetition"]), case_id))
        fixture = report.corpus[case["fixture_id"]]
        spec = next((d for d in _fixture_decisions(fixture) if d["id"] == decision_id), None)
        _require(spec is not None, f"{decision_id} is not a fixture decision")
        assert spec is not None
        expected = row.get("expected")
        _require(expected == spec["expected"], f"{case_id} expected class")
        _require(row.get("distractor") is bool(spec.get("distractor", False)), "distractor flag")
        status = row.get("status")
        _require(status in ("PASS", "FAIL", "ERROR", "NOT_AVAILABLE"), "decision status")
        predicted = _lst(row.get("predicted"), "predicted")
        _require(all(item in CLASSES for item in predicted), "predicted classes")
        _require(len(set(predicted)) == len(predicted), "predicted repeats a class")
        mutations = _lst(row.get("mutations"), "mutations")
        _lst(row.get("attempts"), "attempts")
        duplicates = _int(row.get("correction_duplicates"), "correction duplicates", 0)
        observation = report.value(row.get("evidence_ref"))
        _same_scope(observation, row, "decision evidence")
        observed = _state_diff(observation.get("before"), observation.get("after"))
        recorded = set()
        for raw_mutation in mutations:
            mutation = _obj(raw_mutation, "mutation")
            _require(mutation.get("kind") in DURABLE, "mutation class")
            recorded.add((str(mutation["kind"]), _text(mutation.get("item_id"), "item id")))
        _require(recorded == observed, f"{case_id} mutations disagree with recorded state")
        durable = {kind for kind, _ in observed}
        _require(
            durable == {item for item in predicted if item in DURABLE},
            f"{case_id} predicted durable classes disagree with state",
        )
        _require(duplicates == observation.get("correction_duplicates", 0), "duplicate evidence")
        if status == "NOT_AVAILABLE":
            _require(
                expected in DURABLE and not capabilities[str(expected)],
                f"{case_id} is unavailable without capability absence",
            )
            _require(not set(predicted) & {"clarify", "nothing"}, "unavailable no-write label")
        else:
            soft = set(predicted) - set(DURABLE)
            _require(not (soft and durable), f"{case_id} mixes writes with a no-write label")
            if soft:
                _require(observation.get("semantic") == "PASS", f"{case_id} semantic support")
                _require(soft == {observation.get("semantic_action")}, f"{case_id} semantics")
        forbidden = _forbids_persistence(spec) and bool(row["attempts"])
        if forbidden:
            _require(status != "PASS", f"{case_id} passed despite a forbidden persistence attempt")
        if report.data.get("data_origin") == "live-campaign":
            _require(
                _rederived_predicates(report, spec, observation, capabilities)
                == observation.get("admitted_predicates"),
                f"{case_id} admitted predicates differ from the fixture-derived ones",
            )
            if status in ("PASS", "FAIL"):
                _require(
                    (_recompute_deterministic(observation) and not forbidden)
                    == observation.get("deterministic"),
                    f"{case_id} deterministic outcome is not recomputable from its snapshots",
                )
        if status == "PASS":
            _require(expected in predicted, f"{case_id} passed without its class")
            _require(observation.get("deterministic") is True, f"{case_id} deterministic")
            _require(observation.get("semantic", "PASS") == "PASS", f"{case_id} semantic")
        rows.append(row)
    _require(covered == set(report.cases), "every executed scenario needs a scored decision")
    missing = sorted(_required_decisions(report, capabilities) - seen)
    _require(not missing, f"fixture-derived decisions are missing: {missing[:3]}")
    return rows


def _rederived_predicates(
    report: _Report,
    spec: Mapping[str, Any],
    observation: Mapping[str, Any],
    capabilities: Mapping[str, bool],
) -> list[dict[str, Any]]:
    """Render the pinned fixture predicates with the recorded substitutions and
    admit them against recorded capability evidence and the recorded snapshots."""

    available = {str(k) for k in _lst(observation.get("available"), "available classes")}
    _require(
        available == {k for k in DURABLE if capabilities[k]},
        "decision capability evidence disagrees with discovery",
    )
    substitutions = _obj(observation.get("substitutions"), "substitutions")
    values = {
        str(k): _text(v, "substitution")
        for k, v in _obj(substitutions.get("values"), "values").items()
    }
    literals = {
        str(k): _text(v, "literal")
        for k, v in _obj(substitutions.get("literals"), "literals").items()
    }
    _require(set(literals) == set(CHANNELS), "substituted literals are the fixture channels")
    rendered = _substitute(copy.deepcopy(spec["predicates"]), values, literals)
    before = _obj(observation.get("before"), "before")
    after = _obj(observation.get("after"), "after")
    kept, _ = _admit_predicates(rendered, before, after, available)
    return kept


def _check_judge_bindings(report: _Report, rows: list[dict[str, Any]]) -> None:
    """Every judged decision binds one recorded judge generation whose completion
    parses to the recorded semantic verdict."""

    from .learning_judge import JudgeSpec, _prompt, parse_judge_verdict

    used: set[str] = set()
    live = report.data.get("data_origin") == "live-campaign"
    generations = {
        str(_obj(g, "generation").get("id")): _obj(g, "generation")
        for g in _lst(report.data.get("generations"), "generations")
    }
    for row in rows:
        observation = report.value(row.get("evidence_ref"))
        semantic = observation.get("semantic", "NOT_RUN")
        if row["status"] == "NOT_AVAILABLE" or (semantic == "NOT_RUN" and "judge_ref" not in row):
            continue
        label = f"{row['case_id']} {row['decision_id']}"
        ref = _obj(row.get("judge_ref"), f"{label} judge reference")
        _require(
            ref.get("case_id") == row["case_id"] and ref.get("decision_id") == row["decision_id"],
            f"{label} judge reference is bound to another decision",
        )
        identity = _text(ref.get("generation_id"), f"{label} judge id")
        _require(identity not in used, f"{label} shares a judge generation")
        used.add(identity)
        evidence = _obj(ref.get("evidence"), f"{label} judge evidence")
        refs = _obj(evidence.get("refs"), f"{label} judge evidence refs")
        for key in ("request:teaching", "expected:meaning"):
            _text(refs.get(key), f"{label} judge evidence {key}")
        _require(evidence.get("output") == observation.get("output"), f"{label} judge output")
        if live:
            # Rebuild the exact prompt from the recorded evidence and pinned rubric.
            fixture = report.corpus[str(row["fixture_id"])]
            spec = next(d for d in _fixture_decisions(fixture) if d["id"] == row["decision_id"])
            substitutions = _obj(observation.get("substitutions"), "substitutions")
            rubric = _substitute(
                _decision_rubric(spec),
                _obj(substitutions.get("values"), "values"),
                _obj(substitutions.get("literals"), "literals"),
            )
            prompt = _prompt(
                JudgeSpec(rubric=rubric, evidence=evidence, deterministic=ScoreResult(passed=True))
            )
            _require(_sha(prompt.encode()) == ref.get("prompt_sha256"), f"{label} judge prompt")
        generation = generations.get(identity)
        _require(generation is not None, f"{label} judge generation is unrecorded")
        assert generation is not None
        _require(generation.get("purpose") == "judge", f"{label} judge generation purpose")
        for key in ("call_sequence", "prompt_sha256", "completion_sha256"):
            _require(generation.get(key) == ref.get(key), f"{label} judge {key} binding")
        completion = _text(ref.get("completion"), f"{label} judge completion")
        _require(
            _sha(completion.encode()) == ref.get("completion_sha256"),
            f"{label} judge completion digest",
        )
        if semantic == "ERROR":
            try:
                parse_judge_verdict(completion)
            except ValueError:
                continue
            raise _Invalid(f"{label} records a malformed verdict that parses")
        verdict = parse_judge_verdict(completion)
        _require(verdict.verdict == semantic, f"{label} judge verdict disagrees")
        action = None if verdict.action is None else str(verdict.action)
        _require(action == observation.get("semantic_action"), f"{label} judge action disagrees")


def _recompute_deterministic(observation: Mapping[str, Any]) -> bool:
    """Rerun score_state on the recorded snapshots and admitted predicates."""

    from .learning_metrics import EvidenceError, LearningClass, StateSnapshot, score_state

    predicates = _lst(observation.get("admitted_predicates"), "admitted predicates")
    available = frozenset(
        LearningClass(str(k)) for k in _lst(observation.get("available"), "available classes")
    )

    def state(raw: object, label: str) -> StateSnapshot:
        record = _obj(raw, label)
        return StateSnapshot(
            skills=tuple(_obj(r, "skill row") for r in _lst(record.get("skills"), "skills")),
            facts=tuple(_obj(r, "fact row") for r in _lst(record.get("facts"), "facts")),
            schedules=tuple(
                _obj(r, "schedule row") for r in _lst(record.get("schedules"), "schedules")
            ),
            available=available,
        )

    try:
        result = score_state(
            state(observation.get("before"), "before"),
            state(observation.get("after"), "after"),
            {"predicates": predicates},
        )
    except EvidenceError:
        return False
    return result.passed


def _verdict_failures(report: _Report, rows: list[dict[str, Any]]) -> list[str]:
    """A report never passes while a runnable decision or control failed or errored."""

    failures = [
        f"decision {row['case_id']} {row['decision_id']} is {row['status']}"
        for row in rows
        if row["status"] in ("FAIL", "ERROR")
    ]
    for raw in _lst(report.data.get("controls", []), "controls"):
        control = _obj(raw, "control")
        if control.get("status") in ("FAIL", "ERROR"):
            failures.append(
                f"control {control.get('case_id')} {control.get('control')} is "
                f"{control.get('status')}"
            )
    return failures


def _check_metrics(report: _Report, rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    metrics = _obj(report.data.get("metrics"), "metrics")
    pooled = _summary(rows, report.thresholds)
    for key, value in pooled.items():
        _require(metrics.get(key) == value, f"pooled metric {key} is not recomputable")
    reported = _lst(metrics.get("repetitions"), "per-repetition metrics")
    _require(len(reported) == report.repetitions, "per-repetition metrics are incomplete")
    for repetition in range(1, report.repetitions + 1):
        expected = {
            "repetition": repetition,
            **_summary([row for row in rows if row["repetition"] == repetition], report.thresholds),
        }
        _require(reported[repetition - 1] == expected, f"repetition {repetition} metrics")
        for name, gate in expected["gates"].items():
            if gate["status"] == "FAIL":
                failures.append(f"repetition {repetition} gate {name} failed")
    return failures


def _usage_time(value: object) -> datetime:
    return _utc(_text(value, "poll time"), "poll time")


def _check_actor_settlement(operation: Mapping[str, Any], baseline: Decimal) -> None:
    """A settled actor charge needs two equal usage polls at least 10 s apart that
    explain it; anything less stays unresolved at its full reservation."""

    identity = str(operation["id"])
    polls = _lst(operation.get("usage_polls"), f"{identity} usage polls")
    _require(len(polls) >= 2, f"{identity} settled without a second usage poll")
    last, previous = _obj(polls[-1], "usage poll"), _obj(polls[-2], "usage poll")
    stable = _decimal(last.get("usage_usd"), "poll usage")
    _require(stable == _decimal(previous.get("usage_usd"), "poll usage"), f"{identity} unstable")
    gap = _usage_time(last.get("polled_at")) - _usage_time(previous.get("polled_at"))
    _require(gap >= timedelta(seconds=STABLE_POLL_GAP_SECONDS), f"{identity} polls too close")
    settled = _decimal(operation.get("settled_usd"), "settled")
    _require(Decimal(0) < settled <= stable - baseline, f"{identity} charge is not observed usage")


def _check_worker_and_schedules(report: _Report) -> None:
    worker = _obj(report.data.get("worker"), "worker")
    attempts = worker.get("max_attempts")
    _require(_is_int(attempts) and attempts == 1, "the private worker must make one attempt")
    rows: set[tuple[int, str, str]] = set()
    for raw in _lst(report.data.get("schedule_reservations"), "schedule reservations"):
        row = _obj(raw, "schedule reservation")
        identity = (
            _int(row.get("repetition"), "reservation repetition", 1),
            _text(row.get("case_id"), "reservation case"),
            _text(row.get("schedule_id"), "reservation schedule"),
        )
        _require(identity not in rows, f"duplicate schedule reservation {identity}")
        _require(
            row.get("disposition") in ("reserved", "cancelled"),
            f"schedule {identity[2]} was neither reserved nor cancelled",
        )
        rows.add(identity)
    for raw in _lst(report.data.get("decisions"), "decisions"):
        decision = _obj(raw, "decision")
        for mutation in _lst(decision.get("mutations"), "mutations"):
            if _obj(mutation, "mutation").get("kind") != "schedule":
                continue
            identity = (
                int(decision["repetition"]),
                str(decision["case_id"]),
                str(mutation["item_id"]),
            )
            _require(identity in rows, f"schedule {identity[2]} has no reservation record")


def _check_generations_and_accounting(report: _Report) -> None:
    data = report.data
    models = _obj(data.get("models"), "models")
    _require(models.get("actor") == ACTOR_MODEL, "actor model")
    _require(models.get("judge") == JUDGE_MODEL, "judge model")
    _require(models.get("resolved_hostname") == PROVIDER_HOST, "resolved provider host")
    accounting = _obj(data.get("accounting"), "accounting")
    operations: dict[str, dict[str, Any]] = {}
    settled = Decimal(0)
    for raw in _lst(accounting.get("operations"), "ledger operations"):
        operation = _obj(raw, "ledger operation")
        identity = _text(operation.get("id"), "operation id")
        _require(identity not in operations, f"duplicate operation {identity}")
        _decimal(operation.get("reserve_usd"), "reserve")
        reserve = _decimal(operation.get("reserve_usd"), "reserve")
        amount = _decimal(operation.get("settled_usd"), "settled")
        status = operation.get("status")
        _require(
            status in ("settled", "unresolved", "released"), f"operation {identity} is unsettled"
        )
        if status == "unresolved":
            _require(amount == reserve, f"unknown-cost {identity} must keep its full reservation")
            if "charged_usd" in operation:
                _require(
                    _decimal(operation.get("charged_usd"), "charged") == reserve,
                    f"unknown-cost {identity} must stay charged at its full reservation",
                )
        if status == "released":
            _require(amount == 0, f"released {identity} carries a charge")
            _require(
                not operation.get("generation_id") and not operation.get("record_id"),
                f"released {identity} is bound to a generation",
            )
        if status == "settled" and identity.startswith("actor-"):
            _check_actor_settlement(
                operation, _decimal(accounting.get("baseline_usage_usd"), "baseline usage")
            )
        settled += amount
        operations[identity] = operation
    seen: set[str] = set()
    for raw in _lst(data.get("generations"), "generations"):
        generation = _obj(raw, "generation")
        identity = _text(generation.get("id"), "generation id")
        _require(identity not in seen, f"duplicate generation {identity}")
        seen.add(identity)
        purpose = generation.get("purpose")
        _require(purpose in ("actor", "judge", "calibration", "auxiliary"), "purpose")
        labeled = generation.get("identity_basis") == "configured-route"
        if purpose == "actor":
            _require(generation.get("model") == ACTOR_MODEL, f"{identity} actor model")
            if labeled:
                _require(generation.get("generation_id") is None, f"{identity} invents an id")
                route = _obj(generation.get("route"), f"{identity} route")
                _require(route.get("resolved_host") == PROVIDER_HOST, f"{identity} route host")
                _require(
                    route.get("agent_model_override") == ACTOR_MODEL
                    and route.get("worker_model") == ACTOR_MODEL,
                    f"{identity} configured route model",
                )
                _require(
                    _obj(generation.get("usage"), f"{identity} usage").get("source")
                    == "shared-key-delta",
                    f"{identity} usage source",
                )
            else:
                _require(
                    not identity.startswith("actor-"), f"{identity} looks like a fabricated id"
                )
        else:
            _require(not labeled, f"{identity} judge rows keep provider ids")
            if purpose in ("judge", "calibration"):
                _require(generation.get("model") == JUDGE_MODEL, f"{identity} judge model")
        _obj(generation.get("usage"), f"{identity} usage")
        cost = _decimal(generation.get("cost_usd"), f"{identity} cost")
        bound = operations.get(str(generation.get("ledger_operation_id")))
        _require(bound is not None, f"{identity} has no ledger operation")
        assert bound is not None
        operation = bound
        if labeled:
            _require(
                operation.get("record_id") == identity and not operation.get("generation_id"),
                f"{identity} ledger binding",
            )
        else:
            _require(operation["generation_id"] == identity, f"{identity} ledger binding")
        _require(Decimal(operation["settled_usd"]) == cost, f"{identity} settled cost")
    for case in report.cases.values():
        actor = case.get("actor_generation_id") or case.get("actor_record_id")
        _require(actor in seen, "actor generation is unrecorded")
    budget = _decimal(accounting.get("budget_usd"), "budget")
    _require(0 < budget <= MAX_BUDGET, "campaign budget must be within (0, 5.00]")
    baseline = _decimal(accounting.get("baseline_usage_usd"), "baseline usage")
    end = _decimal(accounting.get("end_usage_usd"), "end usage")
    _require(end >= baseline, "a shared-key usage decrease is never a refund")
    charged = _decimal(accounting.get("charged_usd"), "charged")
    _require(charged >= end - baseline, "final charge is below the campaign usage delta")
    _require(charged >= settled, "charged spend is below settled operations")
    unresolved = _decimal(accounting.get("unresolved_reservations_usd"), "unresolved")
    _require(unresolved == 0, "reservations remain unresolved")
    margin = _decimal(accounting.get("shutdown_and_inflight_reserve_usd"), "shutdown reserve")
    _require(margin > 0, "a final-generation overshoot margin is required")
    _require(charged + unresolved + margin <= budget, "campaign spend exceeds the budget")
    _require(accounting.get("basis") in ACCOUNTING_BASES, "cost basis")


def _check_collector_and_cleanup(report: _Report) -> None:
    collector = _obj(report.data.get("collector"), "collector")
    _int(collector.get("port_b"), "stub port B", 1)
    _require(
        collector.get("bind_errors") == 0 and _is_int(collector.get("bind_errors")),
        "stub bind errors",
    )
    if "recovered_bind_retries" in collector:
        # Informational: a stub bind failure whose single retry succeeded.
        _int(collector["recovered_bind_retries"], "recovered bind retries", 0)
    _delivery_windows(report)
    synthetic = _obj(collector.get("synthetic_transport_receipt"), "synthetic receipt")
    _require(synthetic.get("synthetic") is True, "synthetic receipt label")
    _text(synthetic.get("received_at"), "synthetic receipt time")
    cleanup = _obj(report.data.get("cleanup"), "cleanup")
    _require(cleanup.get("verified") is True, "cleanup was not verified")
    _obj(cleanup.get("ownership_manifest"), "ownership manifest")


_AUX_FIELDS = {
    "procedure_probes": {"task_id", "arm", "successes", "trials", "probe_ids"},
    "memory_negative": {
        "fact_id",
        "deletion_invocation",
        "retention_before",
        "retention_after",
        "before_evidence",
        "after_evidence",
    },
    "deliveries": {
        "schedule_id",
        "due",
        "delivery_count",
        "receipt_times",
        "delivery_ids",
        "schedule_evidence",
    },
}


def _aux_rows(report: _Report, section: str) -> list[dict[str, Any]]:
    container = _obj(report.data.get(section), section)
    _require(set(container) == {"version", "rows"}, f"{section} fields")
    _require(_is_int(container["version"]) and container["version"] == 1, f"{section} version")
    rows = []
    for raw in _lst(container["rows"], f"{section} rows"):
        row = _obj(raw, f"{section} row")
        _require(
            set(row) == {"repetition", "case_id", "agent_id"} | _AUX_FIELDS[section],
            f"{section} row fields",
        )
        _int(row["repetition"], f"{section} repetition", 1)
        _require(row["repetition"] <= report.repetitions, f"{section} repetition beyond N")
        rows.append(row)
    return rows


def _fixture_control(fixture: Mapping[str, Any], control_id: str) -> dict[str, Any] | None:
    for control in fixture.get("controls", []):
        if isinstance(control, dict) and control.get("id") == control_id:
            return control
    return None


def _check_procedures(report: _Report, available: bool) -> list[str]:
    rows = _aux_rows(report, "procedure_probes")
    required: set[tuple[int, str, str]] = set()
    if available:
        for (repetition, _), case in report.cases.items():
            control = _fixture_control(report.corpus[case["fixture_id"]], "removed-skill")
            if control is not None:
                for arm in ("learned", "removed"):
                    required.add((repetition, str(control["task_id"]), arm))
    by_identity: dict[tuple[int, str, str], dict[str, Any]] = {}
    for row in rows:
        case = report.scope_case(row, "F1")
        control = _fixture_control(report.corpus[case["fixture_id"]], "removed-skill")
        _require(control is not None and row["task_id"] == control["task_id"], "task binding")
        _require(row["arm"] in ("learned", "removed"), "procedure arm")
        identity = (int(row["repetition"]), str(row["task_id"]), str(row["arm"]))
        _require(identity not in by_identity, f"duplicate procedure row {identity}")
        by_identity[identity] = row
    _require(set(by_identity) == required, "procedure probe rows do not match the inventory")
    failures: list[str] = []
    improvement = Fraction(Decimal(str(report.thresholds["heldout_control_improvement"])))
    for repetition, task, arm in sorted(required):
        if arm != "learned":
            continue
        learned, removed = (
            by_identity[(repetition, task, "learned")],
            by_identity[(repetition, task, "removed")],
        )
        _require(learned["agent_id"] == removed["agent_id"], "procedure arms share the agent")
        probes: dict[str, list[dict[str, Any]]] = {}
        for row in (learned, removed):
            trials = _int(row["trials"], "trials", 1)
            successes = _int(row["successes"], "successes", 0)
            _require(successes <= trials, "successes exceed trials")
            ids = _lst(row["probe_ids"], "probe ids")
            _require(len(ids) == trials and len(set(map(str, ids))) == trials, "probe ids")
            values = []
            for ref in ids:
                probe = report.value(ref, "procedure-probe")
                _same_scope(probe, row, "procedure probe")
                _require(probe.get("task_id") == task and probe.get("arm") == row["arm"], "arm")
                success = _text(probe.get("expected_output"), "expected output") in str(
                    probe.get("output", "")
                )
                _require(probe.get("success") is success, "probe success is not recomputable")
                _require(probe.get("guidance_sha256") == report.data["guidance_sha256"], "guide")
                _require(probe.get("bundle_sha256") == report.data["bundle_sha256"], "bundle")
                values.append(probe)
            _require(sum(bool(p["success"]) for p in values) == successes, "success count")
            probes[str(row["arm"])] = values
        _require(learned["trials"] == removed["trials"], "procedure arms need equal trials")
        every = probes["learned"] + probes["removed"]
        threads = [_text(p.get("thread"), "probe thread") for p in every]
        inputs = [_text(p.get("input"), "probe input") for p in every]
        _require(len(set(threads)) == len(threads), "procedure probes reuse a thread")
        _require(len(set(inputs)) == len(inputs), "procedure probes reuse an input")
        _require(
            sorted(str(p.get("pair_id")) for p in probes["learned"])
            == sorted(str(p.get("pair_id")) for p in probes["removed"]),
            "probe pairing",
        )
        skill_ids = {p.get("skill_id") for p in every}
        deletions = {p.get("deletion_invocation") for p in every}
        _require(len(skill_ids) == 1 and len(deletions) == 1, "probes target one learned item")
        skill_id = _text(skill_ids.pop(), "skill id")
        deletion = report.invocation(deletions.pop())
        _same_scope(deletion, learned, "skill deletion")
        _require(deletion["exit_code"] == 0, "skill deletion failed")
        argv = deletion["argv"]
        _require("skills" in argv and "delete" in argv and argv[-1] == skill_id, "delete argv")
        for probe in every:
            after = _obj(probe.get("after"), "probe state")
            present = any(row.get("id") == skill_id for row in _lst(after.get("skills"), "skills"))
            _require(present is (probe["arm"] == "learned"), "removal changed the wrong item")
        difference = Fraction(learned["successes"], learned["trials"]) - Fraction(
            removed["successes"], removed["trials"]
        )
        if not (difference > improvement and learned["successes"] >= 1):
            failures.append(f"repetition {repetition} procedure {task} shows no improvement")
    return failures


def _retention(report: _Report, probe: Mapping[str, Any], fact_id: str) -> str:
    snapshot = _obj(report.value(probe.get("snapshot_ref"), "cli-result").get("result"), "facts")
    facts = _lst(snapshot.get("facts"), "facts")
    present = any(_obj(row, "fact").get("id") == fact_id for row in facts)
    value = _text(probe.get("required_value"), "required value")
    return "PASS" if present and value in str(probe.get("output", "")) else "FAIL"


def _check_negative(report: _Report) -> list[str]:
    rows = _aux_rows(report, "memory_negative")
    required = {
        key
        for key, case in report.cases.items()
        if _fixture_control(report.corpus[case["fixture_id"]], "deleted-fact") is not None
    }
    seen: set[tuple[int, str, str]] = set()
    covered: set[tuple[int, str]] = set()
    failures: list[str] = []
    for row in rows:
        report.scope_case(row, "F3")
        fact_id = _text(row["fact_id"], "fact id")
        identity = (int(row["repetition"]), str(row["case_id"]), fact_id)
        _require(identity not in seen, "duplicate memory negative row")
        seen.add(identity)
        covered.add(identity[:2])
        deletion = _obj(row["deletion_invocation"], "deletion invocation")
        _require(set(deletion) == {"invocation_id", "argv", "exit_code", "result_ref"}, "fields")
        record = report.invocation(deletion["invocation_id"])
        _require(_is_int(deletion["exit_code"]) and deletion["exit_code"] == 0, "delete failed")
        for key in ("argv", "exit_code", "result_ref"):
            _require(deletion[key] == record[key], f"deletion {key} disagrees with the record")
        _same_scope(record, row, "deletion invocation")
        argv = record["argv"]
        _require("memory" in argv and row["agent_id"] in argv, "deletion argv agent")
        _require("--delete" in argv and argv[argv.index("--delete") + 1] == fact_id, "fact id")
        result = _obj(report.value(record["result_ref"]).get("result"), "deletion result")
        _require(result.get("deleted") is True and result.get("id") == fact_id, "native delete")
        before = report.value(row["before_evidence"], "retention-probe")
        after = report.value(row["after_evidence"], "retention-probe")
        case = report.cases[identity[:2]]
        for probe in (before, after):
            _same_scope(probe, row, "retention probe")
            _require(probe.get("fact_id") == fact_id, "retention probe fact")
            _require(probe.get("guidance_sha256") == report.data["guidance_sha256"], "guidance")
            _require(probe.get("bundle_sha256") == report.data["bundle_sha256"], "bundle")
            _require(probe.get("state_caller") == "platform", "transcript principal")
            _require("transcript" in probe, "retention transcript")
            _require(probe.get("channel") != probe.get("teaching_channel"), "cross-channel")
            value = _text(probe.get("required_value"), "required value")
            _require(value not in str(probe.get("input", "")), "the answer was in the prompt")
            thread = _text(probe.get("thread"), "probe thread")
            _require(thread != case["_thread"], "probe reused the teaching thread")
            invocation = report.invocation(probe.get("invocation_id"))
            _same_scope(invocation, row, "probe invocation")
            argv = invocation["argv"]
            _require("--thread" in argv and argv[argv.index("--thread") + 1] == thread, "thread")
            _require(
                "--channel" in argv and argv[argv.index("--channel") + 1] == probe.get("channel"),
                "probe channel",
            )
            _require(invocation["result_ref"] == probe.get("result_ref"), "probe result")
            reply = _obj(report.value(invocation["result_ref"]).get("result"), "probe reply")
            _require(reply.get("reply") == probe.get("output"), "probe reply binding")
            _require(probe.get("retention") == _retention(report, probe, fact_id), "retention")
        _require(before["thread"] != after["thread"], "retention probes reuse a thread")
        _require(before["required_value"] == after["required_value"], "required value changed")
        _require(before.get("retention_scorer") == after.get("retention_scorer"), "scorer changed")
        scorer = _obj(before.get("retention_scorer"), "retention scorer")
        _require(scorer.get("required_value") == before["required_value"], "scorer value")
        _require(
            report.order[before["invocation_id"]]
            < report.order[deletion["invocation_id"]]
            < report.order[after["invocation_id"]],
            "deletion is not between the probes",
        )
        after_facts = _lst(
            _obj(report.value(after.get("snapshot_ref")).get("result"), "after facts").get("facts"),
            "after facts",
        )
        _require(all(_obj(f, "fact").get("id") != fact_id for f in after_facts), "fact remains")
        no_recall = before["required_value"] not in str(after.get("output", ""))
        _require(after.get("no_recall") is no_recall, "no-recall is not recomputable")
        output = str(after.get("output", ""))
        fabricated = bool(NONCE_RE.search(output)) or before["required_value"] in output
        _require(
            after.get("no_fabrication") is not fabricated, "no-fabrication is not recomputable"
        )
        _require(row["retention_before"] in ("PASS", "FAIL", "ERROR", "NOT_AVAILABLE"), "enum")
        _require(row["retention_after"] in ("PASS", "FAIL", "ERROR", "NOT_AVAILABLE"), "enum")
        _require(row["retention_before"] == before["retention"], "retention before")
        _require(row["retention_after"] == after["retention"], "retention after")
        if not (
            row["retention_before"] == "PASS"
            and row["retention_after"] == "FAIL"
            and no_recall
            and after.get("no_fabrication") is True
        ):
            failures.append(f"{row['case_id']} memory deletion negative did not flip safely")
    _require(covered == required, "memory negative rows do not match the inventory")
    return failures


def _delivery_windows(report: _Report) -> dict[int, list[tuple[datetime, datetime, int]]]:
    """Collector windows on port B by repetition: (opened, closed, live message children)."""

    collector = _obj(report.data.get("collector"), "collector")
    windows: dict[int, list[tuple[datetime, datetime, int]]] = {}
    for raw in _lst(collector.get("delivery_windows"), "delivery windows"):
        window = _obj(raw, "delivery window")
        _require(
            set(window) == {"repetition", "opened_at", "closed_at", "message_children_alive"},
            "delivery window fields",
        )
        repetition = _int(window["repetition"], "window repetition", 1)
        _require(repetition <= report.repetitions, "window repetition beyond N")
        opened = _utc(window["opened_at"], "window opened_at")
        closed = _utc(window["closed_at"], "window closed_at")
        _require(opened <= closed, "a delivery window closes before it opens")
        alive = _int(window["message_children_alive"], "message children alive", 0)
        windows.setdefault(repetition, []).append((opened, closed, alive))
    return windows


def _check_deliveries(report: _Report, available: bool) -> list[str]:
    rows = _aux_rows(report, "deliveries")
    port_b = report.data["collector"]["port_b"]
    windows = _delivery_windows(report)
    target = int(report.thresholds["scheduled_deliveries"])
    required: set[tuple[int, str, bool]] = set()
    if available:
        for (repetition, case_id), case in report.cases.items():
            if _fixture_control(report.corpus[case["fixture_id"]], "real-clock-delivery"):
                required |= {(repetition, case_id, True), (repetition, case_id, False)}
    seen: set[tuple[int, str]] = set()
    covered: set[tuple[int, str, bool]] = set()
    failures: list[str] = []
    for row in rows:
        report.scope_case(row, "F4")
        schedule_id = _text(row["schedule_id"], "schedule id")
        identity = (int(row["repetition"]), schedule_id)
        _require(identity not in seen, "duplicate delivery row")
        seen.add(identity)
        _require(isinstance(row["due"], bool), "due must be a boolean")
        covered.add((int(row["repetition"]), str(row["case_id"]), bool(row["due"])))
        count = _int(row["delivery_count"], "delivery count", 0)
        ids = _lst(row["delivery_ids"], "delivery ids")
        times = [_utc(item, "receipt time") for item in _lst(row["receipt_times"], "times")]
        _require(count == len(ids) == len(times), "delivery count disagrees with receipts")
        _require(len(set(map(str, ids))) == len(ids), "duplicate final delivery identity")
        _require(times == sorted(times), "receipt times must be nondecreasing")
        observation = report.value(row["schedule_evidence"], "schedule-observation")
        _same_scope(observation, row, "schedule observation")
        _require(observation.get("schedule_id") == schedule_id, "schedule identity")
        snapshot = _lst(
            _obj(observation.get("cli_snapshot"), "snapshot").get("agent_schedules"),
            "agent_schedules",
        )
        stored = [s for s in snapshot if _obj(s, "schedule").get("id") == schedule_id]
        _require(len(stored) == 1, "the schedule is absent from the CLI snapshot")
        state = observation.get("state")
        _require(stored[0].get("state") == state, "observed schedule state disagrees")
        due_at = _utc(observation.get("due_at"), "due time")
        start = _utc(observation.get("observation_start"), "observation start")
        end = _utc(observation.get("observation_end"), "observation end")
        poll = _int(observation.get("normal_poll_seconds"), "poll interval", 1)
        tolerance = _int(observation.get("tolerance_seconds"), "tolerance", 1)
        due = (
            observation.get("kind") == "one-shot"
            and state != "cancelled"
            and start <= due_at <= end
        )
        _require(observation.get("due") is due and row["due"] is due, "due is not recomputable")
        token = _text(observation.get("task_token"), "task token")
        channel = _text(observation.get("channel"), "schedule channel")
        route = _obj(observation.get("delivery_route"), "delivery route")
        _require(set(route) == {"channel", "worker_slack_api_port"}, "delivery route fields")
        _require(route.get("channel") == channel, "delivery route channel")
        _require(
            _is_int(route.get("worker_slack_api_port"))
            and route["worker_slack_api_port"] == port_b,
            "the worker Slack API port must be the collector port B",
        )
        quiet = [(o, c) for o, c, alive in windows.get(int(row["repetition"]), []) if alive == 0]
        if due and not windows.get(int(row["repetition"])):
            failures.append(f"{schedule_id} is due but its repetition has no delivery window")
        for moment in times:
            if not any(o <= moment <= c for o, c in quiet):
                failures.append(f"{schedule_id} receipt lies outside every quiet window")
        receipts = _lst(observation.get("collector_receipts"), "collector receipts")
        _require([_obj(r, "receipt").get("id") for r in receipts] == ids, "receipt identities")
        for receipt, moment in zip(receipts, times, strict=True):
            _require(receipt.get("synthetic") is False, "synthetic posts are not deliveries")
            _require(_utc(receipt.get("received_at"), "receipt") == moment, "receipt time")
            _require(receipt.get("channel") == channel, "delivery channel")
            _require(token in str(receipt.get("text", "")), "delivery token")
            _lst(receipt.get("updates", []), "receipt updates")
        if due:
            _require(state in ("terminal", "done"), "a delivered one-shot must be terminal")
            for moment in times:
                _require(due_at <= moment <= due_at + timedelta(seconds=tolerance), "window")
            if times:
                _require(end >= times[-1] + timedelta(seconds=2 * poll), "duplicate window")
            if count != target:
                failures.append(f"{schedule_id} delivered {count} times, expected {target}")
        else:
            _require(end >= due_at + timedelta(seconds=tolerance), "cancel observation window")
            if count:
                failures.append(f"{schedule_id} delivered after cancellation")
    _require(covered == required, "delivery rows do not match the inventory")
    due_rows = [row for row in rows if row["due"]]
    once = sum(row["delivery_count"] == target for row in due_rows)
    expected = {
        "due_schedules": len(due_rows),
        "delivered_exactly_once": once,
        "delivery_rate": once / len(due_rows) if due_rows else None,
    }
    _require(report.data.get("delivery_metrics") == expected, "delivery metrics")
    if available and expected["delivery_rate"] != 1.0:
        failures.append("scheduled delivery rate is not 100%")
    return failures


def _check_calibration(report: _Report) -> None:
    from .learning_judge import parse_judge_verdict

    path = Path(report.data["fixtures"]["learning-calibration.json"]["path"])
    document = _load_calibration_document(path)
    examples = {str(e["id"]): e for e in document["examples"]}
    calibration = _obj(report.data.get("calibration"), "calibration")
    _require(calibration.get("version") == 1 and _is_int(calibration.get("version")), "version")
    generations = [_obj(g, "generation") for g in _lst(report.data.get("generations"), "gens")]
    operations = _lst(_obj(report.data.get("accounting"), "accounting").get("operations"), "ops")
    rows = _lst(calibration.get("rows"), "calibration rows")
    _require(sorted(str(_obj(r, "row").get("id")) for r in rows) == sorted(examples), "ids")
    bindings: set[tuple[str, int]] = set()
    sequences: set[int] = set()
    for raw in rows:
        row = _obj(raw, "calibration row")
        example = examples[str(row["id"])]
        _require(row.get("group") == example["group"], "calibration group")
        _require(row.get("expected") == example["expected"], "calibration expectation")
        _require(row.get("evidence") == example["evidence"], "calibration evidence")
        _require(row.get("deterministic") == example["deterministic"], "deterministic input")
        _require(row.get("source") == "provider", "calibration must come from the provider")
        prompt = _text(row.get("prompt"), "prompt")
        completion = _text(row.get("completion"), "completion")
        _require(row.get("prompt_sha256") == _sha(prompt.encode()), "prompt digest")
        _require(row.get("completion_sha256") == _sha(completion.encode()), "completion digest")
        sequence = _int(row.get("call_sequence"), "call sequence", 1)
        _require(sequence not in sequences, "call sequence reused")
        sequences.add(sequence)
        key = (str(row["prompt_sha256"]), sequence)
        _require(key not in bindings, "duplicate calibration binding")
        bindings.add(key)
        try:
            verdict = parse_judge_verdict(completion)
        except ValueError as exc:
            raise _Invalid("calibration completion is malformed") from exc
        refs = set(example["evidence"]["refs"])
        _require(set(verdict.evidence_refs) <= refs, "completion cites unknown evidence")
        _require(verdict.verdict == row.get("verdict"), "verdict disagrees with the completion")
        _require(row["verdict"] == example["expected"], f"calibration {row['id']} misjudged")
        matches = [g for g in generations if g.get("id") == row.get("generation_id")]
        _require(len(matches) == 1, "calibration generation binding")
        generation = matches[0]
        _require(generation.get("model") == JUDGE_MODEL == row.get("model"), "judge model")
        _require(generation.get("purpose") == "calibration", "generation purpose")
        _obj(generation.get("usage"), "generation usage")
        for name in ("prompt_sha256", "call_sequence", "completion_sha256", "ledger_operation_id"):
            _require(generation.get(name) == row.get(name), f"generation {name} binding")
        _require(
            any(
                _obj(o, "op").get("id") == row.get("ledger_operation_id")
                and o.get("generation_id") == row.get("generation_id")
                for o in operations
            ),
            "calibration ledger binding",
        )
    controls = _lst(calibration.get("deterministic_controls"), "deterministic controls")
    declared = [_obj(c, "control") for c in document.get("deterministic_controls", [])]
    names = {str(c["group"]) for c in declared} | {str(c["id"]) for c in declared}
    groups_seen: set[str] = set()
    for raw in controls:
        control = _obj(raw, "deterministic control")
        _require(control.get("id") in names, "unknown deterministic control")
        _require(control.get("deterministic") == "FAIL", "deterministic control must fail")
        _require(control.get("judge_called") is False, "deterministic failure reached the judge")
        _require(control.get("verdict") == "FAIL", "deterministic control verdict")
        groups_seen.add(str(control["id"]))
    _require(
        groups_seen >= {str(c["group"]) for c in declared}
        or groups_seen >= {str(c["id"]) for c in declared},
        "deterministic controls are incomplete",
    )


def _docker_lines(*args: str) -> list[str]:
    docker = shutil.which("docker")
    if docker is None:
        raise _Invalid("the caller's Docker client is unavailable")
    completed = subprocess.run(
        [docker, *args],
        capture_output=True,
        timeout=60,
        check=False,
        stdin=subprocess.DEVNULL,
        env=_observer_env(),
    )
    if completed.returncode != 0:
        raise _Invalid("the caller's Docker daemon could not be observed")
    return completed.stdout.decode("utf-8", errors="replace").splitlines()


def _docker_env() -> dict[str, str]:
    allowed = (
        "PATH",
        "HOME",
        "DOCKER_HOST",
        "DOCKER_CONTEXT",
        "DOCKER_CONFIG",
        "DOCKER_CERT_PATH",
        "DOCKER_TLS_VERIFY",
        "SSH_AUTH_SOCK",
        "USER",
        "LANG",
        "XDG_RUNTIME_DIR",
        "TMPDIR",
    )
    env = {name: os.environ[name] for name in allowed if name in os.environ}
    env["COMPOSE_DISABLE_ENV_FILE"] = "1"
    return env


_SECRET_MARKERS = ("TOKEN", "KEY", "SECRET", "PASSWORD", "CREDENTIAL", "AUTH")


def _observer_env() -> dict[str, str]:
    """The caller's environment for read-only Docker observations, minus credentials."""

    env = {
        name: value
        for name, value in os.environ.items()
        if name == "SSH_AUTH_SOCK" or not any(marker in name.upper() for marker in _SECRET_MARKERS)
    }
    env["COMPOSE_DISABLE_ENV_FILE"] = "1"
    return env


def _check_cleanup_live(report: _Report) -> None:
    cleanup = _obj(report.data.get("cleanup"), "cleanup")
    manifest = _obj(cleanup.get("ownership_manifest"), "ownership manifest")
    project = _text(manifest.get("project"), "owned project")
    daemon = _text(manifest.get("daemon_id"), "daemon id")
    for kind in ("containers", "runners", "resolver_containers", "volumes", "networks"):
        key = "name" if kind == "volumes" else "id"
        for raw in _lst(manifest.get(kind), kind):
            entry = _obj(raw, f"{kind} entry")
            _text(entry.get(key), f"{kind} identity")
            _require(entry.get("owner") == project, f"{kind} entry lacks ownership evidence")
    processes = _lst(manifest.get("processes"), "processes")
    observed = _docker_lines("info", "--format", "{{.ID}}")
    _require(observed == [daemon], "the configured Docker daemon is not the recorded one")
    # Other sessions create and remove their own resources on a shared engine, so
    # unrelated inventory is recorded but never judged. Preservation is proven by
    # every harness removal targeting only an identity in the ownership manifest.
    owned: dict[str, set[str]] = {
        kind: {str(e["name" if kind == "volumes" else "id"]) for e in manifest[kind]}
        for kind in ("containers", "runners", "resolver_containers", "volumes", "networks")
    }
    for raw in _lst(manifest.get("images", []), "images"):
        entry = _obj(raw, "images entry")
        _text(entry.get("id"), "image identity")
        _require(
            str(entry.get("owner")) == project or str(entry.get("owner")).startswith(f"{project}-"),
            "images entry lacks ownership evidence",
        )
    owned["images"] = {str(e["id"]) for e in manifest.get("images", [])}
    for raw in _lst(manifest.get("removals"), "removals"):
        removal = _obj(raw, "removal")
        _require(set(removal) == {"kind", "id"}, "removal fields")
        _require(removal["kind"] in owned, "unknown removal kind")
        _require(
            removal["id"] in owned[str(removal["kind"])],
            "a removal targeted an identity outside the ownership manifest",
        )
    inventories = {
        "containers": set(
            _docker_lines("container", "ls", "--all", "--no-trunc", "--format", "{{.ID}}")
        ),
        "volumes": set(_docker_lines("volume", "ls", "--format", "{{.Name}}")),
        "networks": set(_docker_lines("network", "ls", "--no-trunc", "--format", "{{.ID}}")),
    }
    for kind in ("containers", "runners", "resolver_containers", "volumes", "networks"):
        inventory = inventories[
            "containers" if kind in {"runners", "resolver_containers"} else kind
        ]
        key = "name" if kind == "volumes" else "id"
        for entry in manifest[kind]:
            _require(entry[key] not in inventory, f"owned {kind} {entry[key]} still exists")
    images = set(_docker_lines("image", "ls", "--format", "{{.Repository}}:{{.Tag}}"))
    for image in owned["images"]:
        _require(image not in images, f"owned image {image} still exists")
    for raw in processes:
        process = _obj(raw, "process")
        stat = Path(f"/proc/{_int(process.get('pid'), 'pid', 1)}/stat")
        if stat.exists():
            fields = stat.read_text(encoding="utf-8").rsplit(")", 1)[1].split()
            _require(fields[19] != str(process.get("start_time_ticks")), "owned process lives")


def _run_validator(path: Path, checks: Callable[[_Report], list[str]]) -> ScoreResult:
    try:
        report = _load_report(path)
        failures = checks(report)
    except (
        _Invalid,
        OSError,
        UnicodeError,
        ValueError,
        KeyError,
        TypeError,
        IndexError,
        subprocess.SubprocessError,
    ) as exc:
        return ScoreResult(passed=False, detail=_bounded(f"{type(exc).__name__}: {exc}"))
    if failures:
        return ScoreResult(passed=False, detail=_bounded("; ".join(failures)))
    return ScoreResult(passed=True, detail="recomputed evidence satisfies the contract")


def _report_checks(report: _Report) -> list[str]:
    capabilities = _capabilities(report)
    _check_cases(report)
    rows = _check_decisions(report, capabilities)
    failures = _check_metrics(report, rows)
    failures += _verdict_failures(report, rows)
    _check_generations_and_accounting(report)
    _check_worker_and_schedules(report)
    _check_judge_bindings(report, rows)
    _check_collector_and_cleanup(report)
    failures += _check_procedures(report, capabilities["skill"])
    failures += _check_negative(report)
    failures += _check_deliveries(report, capabilities["schedule"])
    _check_calibration(report)
    acceptance = "PASS" if all(capabilities.values()) else "NOT_AVAILABLE"
    _require(report.data.get("acceptance") == acceptance, "acceptance status")
    _require(report.data.get("implementation_verification") == "PASS", "verification status")
    _require(report.data.get("exit_code") == 0 and _is_int(report.data.get("exit_code")), "exit")
    return failures


def _negative_checks(report: _Report) -> list[str]:
    capabilities = _capabilities(report)
    _check_cases(report)
    return _check_negative(report) + _check_procedures(report, capabilities["skill"])


def _calibration_checks(report: _Report) -> list[str]:
    _check_calibration(report)
    return []


def _cleanup_checks(report: _Report) -> list[str]:
    _check_cleanup_live(report)
    return []


def verify_report(path: Path) -> ScoreResult:
    """Recompute every gate of a recorded report; never trust its own verdict."""

    return _run_validator(path, _report_checks)


def verify_calibration(path: Path) -> ScoreResult:
    """Check the judge calibration bindings and outcomes of a recorded report."""

    return _run_validator(path, _calibration_checks)


def verify_negative(path: Path) -> ScoreResult:
    """Check the operator deletion negative and the procedure removal controls."""

    return _run_validator(path, _negative_checks)


def verify_cleanup(path: Path) -> ScoreResult:
    """Observe the caller's Docker daemon for the recorded owned resources."""

    return _run_validator(path, _cleanup_checks)


# Live campaign: provider accounting -----------------------------------------------

SHUTDOWN_RESERVE = Decimal("0.10")
# Native per-agent caps set through `local budget` right after deploy. The runner
# applies the configured cap per session (SDK max_budget_usd, priced from the
# SDK's own table, which prices the pinned actor above the billed rate), so a
# fresh session can spend the whole cap: every actor turn and every expected
# scheduled fire reserves the full cap plus one priced final-generation overshoot.
AGENT_DAILY_USD = Decimal("0.75")
AGENT_OUTPUT_TOKENS = 8000
ACTOR_PROMPT_TOKENS_BOUND = 60000
JUDGE_PROMPT_CHARS_PER_TOKEN = 2
RESERVE_MARGIN = Decimal("1.25")
SETTLE_SECONDS = 60
# An actor turn settles only on two equal shared-key usage polls this far apart.
STABLE_POLL_GAP_SECONDS = 10
STABLE_POLL_SLEEP_SECONDS = 12
STABLE_SETTLE_SECONDS = 120
# A message child binds the Slack stub on port B before it enqueues anything.
STUB_BIND_ERROR = "binding the Slack stub on"
STUB_PORT_WAIT_SECONDS = 180
STUB_PORT_POLL_SECONDS = 5
NORMAL_POLL_SECONDS = 60
DELIVERY_TOLERANCE_SECONDS = 240
SCHEDULE_LEAD_MINUTES = 12
# F4 runs last; its due times start this far after F4 begins so its own nine
# remaining turns (three scenarios, three messages each) finish before any is due.
F4_LEAD_MINUTES = 36
F4_STAGGER_MINUTES = 2
AUTHOR = "U-curie-message"
STRIPPED_AUTH = (
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_BASE_URL",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CURIE_CREDENTIALS",
    "CURIE_MODEL_CREDENTIALS",
    "CURIE_OPENROUTER_API_URL",
)


class _Stop(RuntimeError):
    """Stop admission: budget, accounting, setup or provider failure."""


class _Provider:
    """Direct OpenRouter client. The key stays in memory and leaves only in headers."""

    def __init__(self, key: str) -> None:
        self._key = key

    def _request(self, method: str, path: str, body: object | None = None) -> Any:
        data = None if body is None else json.dumps(body).encode()
        request = urllib.request.Request(
            OPENROUTER_API + path,
            data=data,
            method=method,
            headers={"Authorization": f"Bearer {self._key}", "Content-Type": "application/json"},
        )
        try:
            with urllib.request.urlopen(request, timeout=120) as response:  # noqa: S310
                return _strict_loads(response.read())
        except urllib.error.HTTPError as exc:
            raise _Stop(f"provider {path.split('?')[0]} answered HTTP {exc.code}") from None
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            route = path.split("?")[0]
            raise _Stop(f"provider {route} unreachable: {type(exc).__name__}") from None

    def usage(self) -> Decimal:
        data = _obj(_obj(self._request("GET", "/key"), "key").get("data"), "key data")
        value = data.get("usage")
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise _Stop("provider key usage is not numeric")
        amount = Decimal(str(value))
        if not amount.is_finite() or amount < 0:
            raise _Stop("provider key usage is not finite")
        return amount

    def remaining(self) -> Decimal | None:
        data = _obj(_obj(self._request("GET", "/key"), "key").get("data"), "key data")
        value = data.get("limit_remaining")
        if value is None:
            return None
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise _Stop("provider remaining credit is not numeric")
        return Decimal(str(value))

    def catalog(self) -> dict[str, Any]:
        models = _lst(_obj(self._request("GET", "/models"), "catalog").get("data"), "models")
        found = {str(_obj(m, "model").get("id")): m for m in models}
        for model in (ACTOR_MODEL, JUDGE_MODEL):
            if model not in found:
                raise _Stop(f"pinned model {model} is not in the provider catalog")
            pricing = _obj(found[model].get("pricing"), "pricing")
            for name in ("prompt", "completion"):
                price = Decimal(str(pricing.get(name)))
                if not price.is_finite() or price < 0:
                    raise _Stop(f"pinned model {model} has no usable {name} price")
        return {model: found[model]["pricing"] for model in (ACTOR_MODEL, JUDGE_MODEL)}

    def complete(self, prompt: str) -> dict[str, Any]:
        response = _obj(
            self._request(
                "POST",
                "/chat/completions",
                {
                    "model": JUDGE_MODEL,
                    "max_tokens": JUDGE_MAX_TOKENS,
                    "temperature": 0,
                    "messages": [{"role": "user", "content": prompt}],
                    "usage": {"include": True},
                    "reasoning": {"max_tokens": JUDGE_REASONING_TOKENS},
                },
            ),
            "completion",
        )
        model = str(response.get("model", ""))
        if model != JUDGE_MODEL and not model.startswith(JUDGE_MODEL + "-"):
            raise _Stop("the judge response came from an unexpected model")
        choices = _lst(response.get("choices"), "choices")
        text = _obj(_obj(choices[0], "choice").get("message"), "message").get("content")
        usage = _obj(response.get("usage"), "usage")
        cost = usage.get("cost")
        return {
            "id": _text(response.get("id"), "generation id"),
            "model": JUDGE_MODEL,
            "reported_model": model,
            "text": str(text or ""),
            "usage": {k: v for k, v in usage.items() if k != "cost_details"},
            "cost": None
            if isinstance(cost, bool) or not isinstance(cost, int | float)
            else Decimal(str(cost)),
        }


class _Ledger:
    """One SQLite campaign ledger with conservative admission (Decimal strings)."""

    def __init__(self, path: Path, budget: Decimal, provider: _Provider) -> None:
        self._db = sqlite3.connect(path)
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS operations (id TEXT PRIMARY KEY, purpose TEXT, "
            "reserve TEXT, settled TEXT, status TEXT, generation_id TEXT, scope TEXT, "
            "record_id TEXT, usage_polls TEXT)"
        )
        self._db.commit()
        self.budget = budget
        self.provider = provider
        self.baseline = provider.usage()
        self.last_usage = self.baseline
        self.notes: list[str] = []
        self.sequence = 0
        self.closed: str | None = None

    def _sum(self, column: str, *statuses: str) -> Decimal:
        rows = self._db.execute(
            f"SELECT {column} FROM operations WHERE status IN "  # noqa: S608
            f"({', '.join('?' for _ in statuses)})",
            statuses,
        ).fetchall()
        return sum((Decimal(row[0]) for row in rows), Decimal(0))

    def hold_unresolved(self, identity: str, reason: str) -> None:
        """Keep a reservation whose outcome can no longer be observed charged in
        full: an upper bound that counts in charged spend, never a zero."""

        self._db.execute(
            "UPDATE operations SET settled = reserve, status = 'unresolved' "
            "WHERE id = ? AND status = 'reserved'",
            (identity,),
        )
        self._db.commit()
        self.notes.append(f"{_now()} {identity} unresolved at its full reservation: {reason}")

    def observe(self, usage: Decimal) -> None:
        """Keep a high-water mark of shared-key usage. The provider's counter is
        eventually consistent and can read lower on a later poll; a decrease is an
        accounting note, never a refund and never a reason to lower the mark."""

        if usage < self.last_usage:
            self.notes.append(
                f"{_now()} provider usage read {usage} below the high-water mark {self.last_usage}"
            )
            return
        self.last_usage = usage

    def charged(self) -> Decimal:
        """Observed shared-key delta or the sum of settled and unresolved charges,
        whichever is larger. An unresolved charge keeps its full reservation."""

        return max(self.last_usage - self.baseline, self._sum("settled", "settled", "unresolved"))

    def admit(self, purpose: str, reserve: Decimal, scope: Mapping[str, Any]) -> str:
        if self.closed is not None:
            raise _Stop(f"admission is closed: {self.closed}")
        self.observe(self.provider.usage())
        unresolved = self._sum("reserve", "reserved")
        if self.charged() + unresolved + reserve + SHUTDOWN_RESERVE > self.budget:
            self.closed = "budget exhausted before the next operation"
            raise _Stop(self.closed)
        self.sequence += 1
        identity = f"{purpose}-operation-{self.sequence}"
        self._db.execute(
            "INSERT INTO operations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                identity,
                purpose,
                str(reserve),
                "0",
                "reserved",
                "",
                json.dumps(dict(scope)),
                "",
                "",
            ),
        )
        self._db.commit()
        return identity

    def settle(
        self,
        identity: str,
        before: Decimal,
        cost: Decimal | None,
        *,
        generation: str | None = None,
        record: str | None = None,
    ) -> str:
        """Settle one operation. Unknown cost never becomes zero: with no provider
        cost and no observed usage movement the full reservation stays charged."""

        deadline = time.monotonic() + SETTLE_SECONDS
        usage = self.provider.usage()
        while cost is None and usage <= before and time.monotonic() < deadline:
            time.sleep(3)
            usage = self.provider.usage()
        self.observe(usage)
        delta = max(Decimal(0), self.last_usage - before)
        status = "settled"
        if cost is not None:
            settled = max(cost, delta)
        elif delta > 0:
            settled = delta
        else:
            row = self._db.execute(
                "SELECT reserve FROM operations WHERE id = ?", (identity,)
            ).fetchone()
            settled = Decimal(row[0])
            status = "unresolved"
            self.notes.append(f"{_now()} {identity} cost unknown; full reservation stays charged")
        self._db.execute(
            "UPDATE operations SET settled = ?, status = ?, generation_id = ?, record_id = ? "
            "WHERE id = ?",
            (str(settled), status, generation or "", record or "", identity),
        )
        self._db.commit()
        return str(settled)

    def fits(self, reserve: Decimal) -> bool:
        """Whether one more reservation fits the budget, without closing admission."""

        unresolved = self._sum("reserve", "reserved")
        return (
            self.closed is None
            and self.charged() + unresolved + reserve + SHUTDOWN_RESERVE <= self.budget
        )

    def settle_stable(self, identity: str, before: Decimal, *, record: str) -> str:
        """Settle an actor turn only when shared-key usage moved above ``before`` and
        then read equal on two polls at least STABLE_POLL_GAP_SECONDS apart; otherwise
        the full reservation stays charged as unresolved."""

        polls: list[dict[str, str]] = []
        deadline = time.monotonic() + STABLE_SETTLE_SECONDS
        stable: Decimal | None = None
        previous: Decimal | None = None
        while True:
            usage = self.provider.usage()
            self.observe(usage)
            polls.append({"polled_at": _now(), "usage_usd": str(usage)})
            if previous is not None and usage == previous and usage > before:
                stable = usage
                break
            if time.monotonic() > deadline:
                break
            previous = usage
            time.sleep(STABLE_POLL_SLEEP_SECONDS)
        if stable is not None:
            settled, status = stable - before, "settled"
        else:
            row = self._db.execute(
                "SELECT reserve FROM operations WHERE id = ?", (identity,)
            ).fetchone()
            settled, status = Decimal(row[0]), "unresolved"
            self.notes.append(
                f"{_now()} {identity} usage never stabilized; reservation stays charged"
            )
        self._db.execute(
            "UPDATE operations SET settled = ?, status = ?, record_id = ?, usage_polls = ? "
            "WHERE id = ?",
            (str(settled), status, record, json.dumps(polls), identity),
        )
        self._db.commit()
        return str(settled)

    def release(self, identity: str, reason: str) -> None:
        """Release a reservation whose work provably did not run: a cancelled fire, or
        a message child that failed binding its stub before enqueueing anything."""

        self._db.execute(
            "UPDATE operations SET settled = '0', status = 'released' WHERE id = ?", (identity,)
        )
        self._db.commit()
        self.notes.append(f"{_now()} {identity} released: {reason}")

    def fail(self, identity: str, reason: str) -> None:
        """An unresolved charge closes admission and keeps its reservation charged."""

        self.closed = reason
        row = self._db.execute(
            "SELECT reserve FROM operations WHERE id = ?", (identity,)
        ).fetchone()
        if row is not None:
            self._db.execute(
                "UPDATE operations SET settled = ?, status = 'unresolved' WHERE id = ?",
                (row[0], identity),
            )
            self._db.commit()

    def operations(self) -> list[dict[str, Any]]:
        rows = self._db.execute(
            "SELECT id, generation_id, reserve, settled, status, record_id, purpose, usage_polls "
            "FROM operations ORDER BY rowid"
        ).fetchall()
        return [
            {
                "id": r[0],
                "generation_id": r[1] or None,
                "record_id": r[5] or None,
                "purpose": r[6],
                "reserve_usd": r[2],
                "settled_usd": r[3],
                "charged_usd": r[3],
                "status": r[4],
                **({"usage_polls": json.loads(r[7])} if r[7] else {}),
            }
            for r in rows
        ]

    def final(self) -> dict[str, Any]:
        for (identity,) in self._db.execute(
            "SELECT id FROM operations WHERE status = 'reserved'"
        ).fetchall():
            self.hold_unresolved(identity, "still reserved at campaign end")
        deadline = time.monotonic() + 60
        usage = self.provider.usage()
        while time.monotonic() < deadline:
            time.sleep(10)
            later = self.provider.usage()
            if later == usage:
                break
            usage = later
        self.last_usage = max(usage, self.last_usage)
        return {
            "budget_usd": str(self.budget),
            "baseline_usage_usd": str(self.baseline),
            "end_usage_usd": str(self.last_usage),
            "charged_usd": str(self.charged()),
            "unresolved_reservations_usd": str(self._sum("reserve", "reserved")),
            "unknown_cost_charged_usd": str(self._sum("settled", "unresolved")),
            "shutdown_and_inflight_reserve_usd": str(SHUTDOWN_RESERVE),
            "operations": self.operations(),
            "basis": "conservative-shared-key-upper-bound",
            "closed": self.closed,
            "notes": list(self.notes),
        }


# Live campaign: external Slack-compatible collector ---------------------------------
#
# A minimal Slack-compatible post/update collector. It binds the worker's one
# Slack API port B only inside a delivery window, after every `local message`
# child (which binds its own stub on B) has exited. No reusable schedule
# HTTP-channel helper exists in the source today; whichever of this collector
# and a future shared helper lands second should dedupe them.


class _Collector:
    def __init__(self, path: Path, host: str, port: int) -> None:
        self.db_path = path
        self.lock = threading.Lock()
        with self.lock, sqlite3.connect(path) as db:
            db.execute(
                "CREATE TABLE IF NOT EXISTS receipts (id TEXT PRIMARY KEY, channel TEXT, "
                "text TEXT, received_at TEXT, synthetic INTEGER, updates TEXT)"
            )
        collector = self

        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
                return

            def do_POST(self) -> None:  # noqa: N802
                length = int(self.headers.get("Content-Length") or 0)
                raw = self.rfile.read(min(length, 1_000_000))
                body: dict[str, Any] = {}
                with contextlib.suppress(ValueError, UnicodeError):
                    if "json" in (self.headers.get("Content-Type") or ""):
                        parsed = json.loads(raw.decode())
                        body = parsed if isinstance(parsed, dict) else {}
                    else:
                        body = {k: v[-1] for k, v in urllib.parse.parse_qs(raw.decode()).items()}
                method = self.path.rstrip("/").rsplit("/", 1)[-1]
                synthetic = self.headers.get("X-Learning-Synthetic") == "1"
                reply = collector.record(method, body, synthetic)
                data = json.dumps(reply).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        self.server = http.server.ThreadingHTTPServer((host, port), Handler)
        self.port = int(self.server.server_address[1])
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def alive(self) -> bool:
        return self.thread.is_alive()

    def record(self, method: str, body: Mapping[str, Any], synthetic: bool) -> dict[str, Any]:
        channel = str(body.get("channel", ""))
        text = str(body.get("text", ""))
        blocks = body.get("blocks")
        if isinstance(blocks, str):
            text = f"{text} {blocks}"
        now = _now()
        with self.lock, sqlite3.connect(self.db_path) as db:
            if method == "chat.update":
                ts = str(body.get("ts", ""))
                row = db.execute("SELECT updates FROM receipts WHERE id = ?", (ts,)).fetchone()
                if row is not None:
                    updates = json.loads(row[0])
                    updates.append({"received_at": now, "text": text})
                    # An update edits the same final post; keep its newest text.
                    db.execute(
                        "UPDATE receipts SET updates = ?, text = ? WHERE id = ?",
                        (json.dumps(updates), text, ts),
                    )
                return {"ok": True, "channel": channel, "ts": ts}
            if method not in ("chat.postMessage", "chat.postEphemeral"):
                return {"ok": True}
            ts = f"{time.time():.6f}"
            while db.execute("SELECT 1 FROM receipts WHERE id = ?", (ts,)).fetchone():
                ts = f"{float(ts) + 0.000001:.6f}"
            db.execute(
                "INSERT INTO receipts VALUES (?, ?, ?, ?, ?, ?)",
                (ts, channel, text, now, int(synthetic), "[]"),
            )
        return {"ok": True, "channel": channel, "ts": ts, "message": {"ts": ts, "text": text}}

    def receipts(self) -> list[dict[str, Any]]:
        with self.lock, sqlite3.connect(self.db_path) as db:
            rows = db.execute(
                "SELECT id, channel, text, received_at, synthetic, updates FROM receipts "
                "ORDER BY received_at, id"
            ).fetchall()
        return [
            {
                "id": r[0],
                "channel": r[1],
                "text": r[2],
                "received_at": r[3],
                "synthetic": bool(r[4]),
                "updates": json.loads(r[5]),
            }
            for r in rows
        ]

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=10)


# Live campaign: pinned CLI, private stack and ownership ------------------------------


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


_PUBLISHED = {
    "postgres": [5432],
    "valkey": [6379],
    "clickhouse": [8123, 9000],
    "rustfs": [9000, 9001],
    "langfuse-web": [3000],
    "otel-collector": [4317, 4318, 8888],
    "curie-api": [8000],
    "curie-ui": [8080],
}


@dataclass
class _Stack:
    project: str
    tag: str
    override: Path
    ports: dict[str, dict[int, int]]
    stub_port: int
    stub_host: str
    repetition: int = 0
    # The stack's platform API key: generated per stack, held in memory only,
    # handed to `local up` and every inner CLI child as CURIE_API_KEY (which
    # `local up` adopts instead of generating its own), never in argv or reports.
    api_key: str = field(default_factory=lambda: secrets.token_hex(32), repr=False)

    @property
    def api_url(self) -> str:
        return f"http://localhost:{self.ports['curie-api'][8000]}"

    def env(self, base: Path) -> dict[str, str]:
        return {
            "COMPOSE_PROJECT_NAME": self.project,
            "COMPOSE_FILE": f"{base}:{self.override}",
            "CURIE_API_URL": self.api_url,
            "VALKEY_HOST": "localhost",
            "VALKEY_PORT": str(self.ports["valkey"][6379]),
            "S3_ENDPOINT_URL": f"http://localhost:{self.ports['rustfs'][9000]}",
            "CURIE_DOCKER_NETWORK": f"{self.project}_runner",
            "CURIE_LOCAL_STUB_PORT": str(self.stub_port),
            "CURIE_LOCAL_POSTGRES_HOST": "localhost",
            "CURIE_LOCAL_POSTGRES_PORT": str(self.ports["postgres"][5432]),
            "CURIE_LOCAL_IMAGE_TAG": self.tag,
        }

    def write_override(self) -> None:
        lines = ["services:"]
        for service, mapping in self.ports.items():
            bindings = [f"127.0.0.1:{host}:{inner}" for inner, host in mapping.items()]
            lines += [f"  {service}:", "    ports: !override " + json.dumps(bindings)]
        worker = {
            "DATABASE_URL": "postgresql+asyncpg://postgres:${CURIE_LOCAL_POSTGRES_PASSWORD:-"
            f"postgres}}@localhost:{self.ports['postgres'][5432]}/postgres",
            "VALKEY_HOST": "localhost",
            "VALKEY_PORT": str(self.ports["valkey"][6379]),
            "S3_ENDPOINT_URL": f"http://localhost:{self.ports['rustfs'][9000]}",
            "CURIE_API_URL": self.api_url,
            "SLACK_API_BASE_URL": f"http://{self.stub_host}:{self.stub_port}/api/",
            "CURIE_DOCKER_NETWORK": f"{self.project}_runner",
            "OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{self.ports['otel-collector'][4318]}",
            # One attempt per ordinary turn: a reservation covers one session, and
            # the kernel would otherwise retry a failed turn up to three times.
            "CURIE_MAX_ATTEMPTS": "1",
        }
        lines += ["  curie-worker:", "    environment:"]
        lines += [f"      {key}: {json.dumps(value)}" for key, value in worker.items()]
        lines += [
            "networks:",
            "  default:",
            f"    name: {self.project}_default",
            "  curie_runner:",
            f"    name: {self.project}_runner",
        ]
        self.override.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _stub_advertise_host() -> str:
    """The caller-provided stub host the worker reaches; localhost only when unset."""

    return os.environ.get("CURIE_LOCAL_STUB_HOST") or "localhost"


def _stub_bind_host() -> str:
    """Bind exactly as `local message` binds its stub (resolve_local_stub_binding)."""

    if os.environ.get("CURIE_LOCAL_STUB_HOST") or sys.platform == "darwin":
        return "0.0.0.0"  # noqa: S104
    return "127.0.0.1"


def _new_stack(
    output: Path, run_id: str, repetition: int, stub_port: int, attempt: int = 1
) -> _Stack:
    used = {stub_port}
    ports: dict[str, dict[int, int]] = {}
    for service, inner in _PUBLISHED.items():
        ports[service] = {}
        for port in inner:
            host = _free_port()
            while host in used:
                host = _free_port()
            used.add(host)
            ports[service][port] = host
    project = f"curie-learning-{run_id}-r{repetition}a{attempt}"
    return _Stack(
        project,
        f"learning-{run_id}",
        output / f"compose-override-r{repetition}a{attempt}.yaml",
        ports,
        stub_port,
        _stub_advertise_host(),
        repetition,
    )


PLATFORM_IMAGE_PREFIX = "ghcr.io/curie-eng/curie-"


@dataclass
class _Ownership:
    project_prefix: str
    daemon_id: str = ""
    containers: dict[str, str] = field(default_factory=dict)
    runners: dict[str, str] = field(default_factory=dict)
    resolvers: dict[str, str] = field(default_factory=dict)
    volumes: dict[str, str] = field(default_factory=dict)
    networks: dict[str, str] = field(default_factory=dict)
    images: dict[str, str] = field(default_factory=dict)
    projects: set[str] = field(default_factory=set)
    tags: set[str] = field(default_factory=set)
    unrelated: dict[str, list[str]] = field(default_factory=dict)
    removals: list[dict[str, str]] = field(default_factory=list)

    def remove(self, kind: str, identity: str, *args: str) -> None:
        """Every removal the harness issues targets one recorded owned identity."""

        self.removals.append({"kind": kind, "id": identity})
        _docker_lines(*args, identity)

    def observe(self, stack: _Stack) -> None:
        label = f"label=com.docker.compose.project={stack.project}"
        for item in _docker_lines(
            "container", "ls", "--all", "--no-trunc", "--filter", label, "--format", "{{.ID}}"
        ):
            self.containers[item] = stack.project
        for item in _docker_lines(
            "container",
            "ls",
            "--all",
            "--no-trunc",
            "--filter",
            f"network={stack.project}_runner",
            "--format",
            "{{.ID}}",
        ):
            if item not in self.containers:
                self.runners[item] = stack.project
        for item in _docker_lines("volume", "ls", "--filter", label, "--format", "{{.Name}}"):
            self.volumes[item] = stack.project
        for item in _docker_lines(
            "network", "ls", "--no-trunc", "--filter", label, "--format", "{{.ID}}"
        ):
            self.networks[item] = stack.project
        self.projects.add(stack.project)
        self.tags.add(stack.tag)
        self.observe_images()

    def observe_images(self) -> None:
        """Record every image a harness stack built: compose names images for
        services with a build section and no image as `<project>-<service>`, and
        `local up --build` tags the platform images with the stack's private tag."""

        for item in _docker_lines("image", "ls", "--format", "{{.Repository}}:{{.Tag}}"):
            repository, _, tag = item.rpartition(":")
            owner = next((p for p in self.projects if repository.startswith(f"{p}-")), None)
            if owner is None and repository.startswith(PLATFORM_IMAGE_PREFIX) and tag in self.tags:
                owner = self.project_prefix
            if owner is not None:
                self.images[item] = owner

    def remove_leftovers(self, *, images: bool = True) -> list[str]:
        problems: list[str] = []
        live = set(_docker_lines("container", "ls", "--all", "--no-trunc", "--format", "{{.ID}}"))
        for kind, owned in (
            ("containers", self.containers),
            ("runners", self.runners),
            ("resolver_containers", self.resolvers),
        ):
            for identity in owned:
                if identity in live:
                    with contextlib.suppress(_Invalid):
                        self.remove(kind, identity, "container", "rm", "--force", "--volumes")
        volumes = set(_docker_lines("volume", "ls", "--format", "{{.Name}}"))
        for name in self.volumes:
            if name in volumes:
                with contextlib.suppress(_Invalid):
                    self.remove("volumes", name, "volume", "rm", "--force")
        networks = set(_docker_lines("network", "ls", "--no-trunc", "--format", "{{.ID}}"))
        for identity in self.networks:
            if identity in networks:
                with contextlib.suppress(_Invalid):
                    self.remove("networks", identity, "network", "rm")
        if images:
            with contextlib.suppress(_Invalid):
                self.observe_images()
            present = set(_docker_lines("image", "ls", "--format", "{{.Repository}}:{{.Tag}}"))
            for image in sorted(self.images):
                if image not in present:
                    continue
                try:
                    self.remove("images", image, "image", "rm")
                except _Invalid:
                    problems.append(f"owned image {image} could not be removed")
        return problems

    def manifest(self) -> dict[str, Any]:
        def rows(items: Mapping[str, str], key: str = "id") -> list[dict[str, str]]:
            return [{key: identity, "owner": self.project_prefix} for identity in sorted(items)]

        return {
            "project": self.project_prefix,
            "daemon_id": self.daemon_id,
            "containers": rows(self.containers),
            "runners": rows(self.runners),
            "resolver_containers": rows(self.resolvers),
            "volumes": rows(self.volumes, "name"),
            "networks": rows(self.networks),
            "processes": [],
            "images": rows(self.images),
            "removals": list(self.removals),
        }


# Live campaign: orchestration ------------------------------------------------------


@dataclass
class _Options:
    curie_bin: Path
    repetitions: int
    budget: Decimal
    output_dir: Path
    thresholds: dict[str, Any]
    dry_run: bool
    json: bool


_PERSISTENCE_TOOLS = ("memory", "remember", "forget", "skill", "schedule")


def _admit_predicates(
    predicates: Sequence[Mapping[str, Any]],
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    available: set[str],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Admit a decision's rendered fixture predicates against the capability
    evidence and the stored fields the before and after snapshots carry."""

    kept: list[dict[str, Any]] = []
    unavailable: list[dict[str, Any]] = []
    for raw in predicates:
        predicate = dict(raw)
        kind = str(predicate["class"])
        if kind not in available:
            unavailable.append({**predicate, "reason": "capability absent"})
            continue
        store = {"skill": "skills", "schedule": "schedules"}.get(kind, "facts")
        rows = [
            r
            for r in [*before[store], *after[store]]
            if store != "facts" or (r.get("kind") == "instruction") == (kind == "instruction")
        ]
        name = str(predicate["name"])
        needed: list[str] = []
        if name in _NEEDED_FIELD:
            needed = [_NEEDED_FIELD[name]]
        elif name == "author_equals":
            needed = ["created_by" if kind == "schedule" else "author"]
        elif name == "schedule_matches":
            needed = list(_obj(predicate["value"], "schedule match"))
        missing = [f for f in needed if any(f not in row for row in rows)]
        if missing:
            unavailable.append({**predicate, "reason": f"absent stored field {missing[0]}"})
        else:
            kept.append(predicate)
    return kept, unavailable


def _forbids_persistence(spec: Mapping[str, Any]) -> bool:
    return spec.get("forbid_persistence_attempts") is True


_NEEDED_FIELD = {
    "statement_equals": "statement",
    "scope_equals": "scope",
    "channel_equals": "channel",
    "body_contains_steps": "body",
    "revision_increased": "revision",
}


def _substitute(
    value: Any, values: Mapping[str, str], literals: Mapping[str, str] | None = None
) -> Any:
    if isinstance(value, str):
        for key, replacement in values.items():
            value = value.replace("{" + key + "}", replacement)
        for literal, replacement in (literals or {}).items():
            value = value.replace(literal, replacement)
        return value
    if isinstance(value, list):
        return [_substitute(item, values, literals) for item in value]
    if isinstance(value, dict):
        return {key: _substitute(item, values, literals) for key, item in value.items()}
    return value


def _scenario_channels(run_id: str, repetition: int, fixture_id: str) -> tuple[str, str]:
    """Two obviously synthetic Slack channel IDs unique per run, repetition and scenario."""

    def one(index: int) -> str:
        digest = hashlib.sha256(f"{run_id}:{repetition}:{fixture_id}:{index}".encode())
        return "C0" + digest.hexdigest()[:9].upper()

    return one(1), one(2)


def _scenario_values(due: datetime) -> dict[str, str]:
    def token(prefix: str) -> str:
        return f"{prefix}-{secrets.token_hex(3)}"

    values = {
        "author": AUTHOR,
        "zone": "Etc/UTC",
        "procedure_name": token("proc"),
        "consent_procedure_name": token("proc"),
        "row_marker": token("rm"),
        "route_marker": token("rt"),
        "check_marker": token("ck"),
        "mass_route": token("route"),
        "length_route": token("route"),
        "closing_marker": token("end"),
        "corrected_marker": token("end"),
        "subject": token("cabinet"),
        "value": token("violet"),
        "corrected_value": token("amber"),
        "channel_subject": token("shelf"),
        "channel_value": token("teal"),
        "schedule_token": token("tok"),
        "cancel_token": token("tok"),
        "revised_token": token("tok"),
    }
    due = due.replace(second=0, microsecond=0)
    cancel = due + timedelta(minutes=3)
    values["due_local"] = f"{due:%Y-%m-%d %H:%M}"
    values["due_utc"] = f"{due:%Y-%m-%dT%H:%M:%S}Z"
    values["cancel_due_local"] = f"{cancel:%Y-%m-%d %H:%M}"
    return values


class _Campaign:
    def __init__(self, options: _Options, key: str | None) -> None:
        self.o = options
        self.key = key
        self.root = SOURCE_ROOT
        self.base_compose = SOURCE_ROOT / "compose.dev.yaml"
        self.run_id = secrets.token_hex(4)
        self.binary: dict[str, Any] = {}
        self.source_manifest: list[dict[str, str]] = []
        self.source_sha = ""
        self.evidence: dict[str, Any] = {}
        self.invocations: list[dict[str, Any]] = []
        self.cases: list[dict[str, Any]] = []
        self.decisions: list[dict[str, Any]] = []
        self.generations: list[dict[str, Any]] = []
        self.controls: list[dict[str, Any]] = []
        self.procedure_rows: list[dict[str, Any]] = []
        self.negative_rows: list[dict[str, Any]] = []
        self.delivery_rows: list[dict[str, Any]] = []
        self.calibration: dict[str, Any] = {"version": 1, "rows": [], "deterministic_controls": []}
        self.failures: list[str] = []
        self.capabilities: dict[str, dict[str, Any]] = {}
        self.memory_schema: dict[str, Any] = {}
        self.stack: _Stack | None = None
        self.ownership = _Ownership(f"curie-learning-{self.run_id}")
        self.ledger: _Ledger | None = None
        self.provider = _Provider(key) if key else None
        self.collector: _Collector | None = None
        self.stub_port = 0
        self.threads: set[str] = set()
        self.bind_errors = 0
        self.recovered_bind_retries = 0
        self.live_children = 0
        self.prices: dict[str, dict[str, Decimal]] = {}
        self.agent_caps: dict[str, dict[str, Any]] = {}
        self.unbound_fires: dict[str, list[str]] = {}
        self.bound_fires: dict[tuple[str, str], str] = {}
        self.open_fires: set[str] = set()
        self.substitutions: dict[str, Any] = {}
        self.schedule_reservations: list[dict[str, Any]] = []
        self.quiesce_actions: list[dict[str, Any]] = []
        self.worker: dict[str, Any] = {}
        self.paid_started = False
        self.setup_retries: list[dict[str, Any]] = []
        self.channels: tuple[str, str] = CHANNELS
        self.platform_keys: list[str] = []
        self.windows: list[dict[str, Any]] = []
        self.f4_base: datetime | None = None
        self.synthetic: dict[str, Any] = {}
        self.call_sequence = 0
        self.last_verdict: Any = None
        self.stopped: str | None = None
        self.resolved: dict[str, Any] = {}
        self.pending: list[dict[str, Any]] = []
        self.window_usage = Decimal(0)
        corpus_path = FIXTURES / "learning-cases.json"
        self.guidance, self.corpus = _load_corpus(corpus_path)
        self.calibration_doc = _load_calibration_document(FIXTURES / "learning-calibration.json")
        self.guidance_sha = _sha(str(self.guidance["text"]).encode())
        self.bundle_sha = _bundle_sha(FIXTURES / "learning-bundle")

    # Evidence and pinned CLI ------------------------------------------------------

    def add(self, kind: str, value: dict[str, Any]) -> str:
        identity = f"{kind}-{len(self.evidence) + 1}"
        self.evidence[identity] = {"kind": kind, "sha256": "", "value": value}
        return identity

    def identity(self) -> None:
        path = self.o.curie_bin
        self.binary = {
            "path": str(path),
            "sha256": _file_sha(path),
            "version": _version_of(str(path)),
        }
        self.source_manifest = _source_manifest(self.root)
        self.source_sha = _sha(_json_bytes(self.source_manifest))

    def recheck_identity(self) -> None:
        if _file_sha(self.o.curie_bin) != self.binary["sha256"]:
            raise _Stop("the pinned executable changed during the run")
        if _sha(_json_bytes(_source_manifest(self.root))) != self.source_sha:
            raise _Stop("the candidate source tree changed during the run")

    def child_env(self, *, credentials: bool = False) -> dict[str, str]:
        env = _docker_env()
        for name in ("CURIE_LOCAL_STUB_HOST", "CURIE_SLACK_TRUSTED_ORIGINS", "NO_COLOR"):
            if name in os.environ:
                env[name] = os.environ[name]
        if self.stack is not None:
            env.update(self.stack.env(self.base_compose))
            env["CURIE_API_KEY"] = self.stack.api_key
        env["CURIE_MODEL"] = ACTOR_MODEL
        if credentials and self.key:
            env["CURIE_CREDENTIALS"] = self.key
            env["CURIE_FAKE_MODEL"] = "0"
        return env

    def scrub(self, text: str) -> str:
        for secret in [self.key, *self.platform_keys]:
            if secret:
                text = text.replace(secret, "[redacted]")
        return _tail(text)

    def cli(
        self,
        args: Sequence[str],
        scope: Mapping[str, Any],
        *,
        cwd: Path | None = None,
        credentials: bool = False,
        timeout: int = 900,
        extra: Mapping[str, Any] | None = None,
    ) -> tuple[int, Any, str, str, str]:
        argv = [str(self.o.curie_bin), "--json", *args]
        workdir = cwd or self.root
        try:
            completed = subprocess.run(
                argv,
                cwd=workdir,
                env=self.child_env(credentials=credentials),
                capture_output=True,
                timeout=timeout,
                check=False,
                stdin=subprocess.DEVNULL,
            )
            code, stdout, stderr = completed.returncode, completed.stdout, completed.stderr
        except subprocess.TimeoutExpired:
            code, stdout, stderr = 124, b"", b"timed out"
        parsed: Any = None
        with contextlib.suppress(_Invalid, ValueError, UnicodeError):
            parsed = _strict_loads(stdout) if stdout.strip() else None
        result_ref = self.add(
            "cli-result", {**scope, "result": parsed, "stdout_sha256": _sha(stdout)}
        )
        invocation_id = f"invocation-{len(self.invocations) + 1}"
        record = {
            **scope,
            "invocation_id": invocation_id,
            "argv": argv,
            "exit_code": code,
            "binary": dict(self.binary),
            "source_root": str(self.root),
            "source_tree_sha256": self.source_sha,
            "cwd": str(workdir),
            "result_ref": result_ref,
        }
        if extra:
            record.update(extra)
        error = self.scrub(
            stderr.decode("utf-8", errors="replace")
            + ("" if parsed is not None else "\n" + stdout.decode("utf-8", errors="replace"))
        )
        if code != 0:
            # Why the child failed, scrubbed of every secret, so a turn that did not
            # finalize shows its cause in the report.
            cause = error.strip()
            if isinstance(parsed, dict) and parsed.get("error") is not None:
                cause = f"{cause} {self.scrub(json.dumps(parsed['error']))}".strip()
            record["error"] = _bounded(cause[-2000:]) or "no error output"
        self.invocations.append(record)
        return code, parsed, invocation_id, result_ref, error

    def must(self, args: Sequence[str], scope: Mapping[str, Any], **kw: Any) -> Any:
        code, parsed, _, _, stderr = self.cli(args, scope, **kw)
        if code != 0:
            detail = stderr.strip()
            if isinstance(parsed, dict) and parsed.get("error") is not None:
                detail = f"{detail} {self.scrub(json.dumps(parsed['error']))}".strip()
            raise _Stop(f"`{' '.join(args[:3])}` failed: {detail or 'no error output'}")
        return parsed

    def api(self, method: str, path: str, body: object | None = None) -> tuple[int, Any]:
        assert self.stack is not None
        request = urllib.request.Request(
            self.stack.api_url + path,
            method=method,
            data=None if body is None else json.dumps(body).encode(),
            headers={
                "X-API-Key": self.stack.api_key,
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310
                raw = response.read()
                return response.status, _strict_loads(raw) if raw.strip() else None
        except urllib.error.HTTPError as exc:
            return exc.code, None
        except (urllib.error.URLError, TimeoutError, OSError):
            return 0, None

    # Discovery, preflight and calibration -----------------------------------------

    def discover(self) -> None:
        scope = {"repetition": 0}
        manifest = self.must(["schema"], scope)
        schema = self.must(["schema-index", "memory"], scope)
        text = json.dumps(manifest)
        try:
            enum = _kind_enum(_obj(schema, "memory schema"))
        except _Invalid as exc:
            raise _Stop(str(exc)) from exc
        learning = "agent-wide-learning" in text
        flags = {
            "skill": "learned-skills" in text,
            "schedule": "agent-schedules" in text,
            # A standing instruction needs the kind in the schema and the opt-in.
            "instruction": "instruction" in (enum or []) and learning,
            "agent_wide_learning": learning,
        }
        discovery = self.add(
            "discovery",
            {
                "binary": dict(self.binary),
                "source_tree_sha256": self.source_sha,
                "exit_code": 0,
                "memory_schema": schema,
                "memory_schema_sha256": _sha(_json_bytes(schema)),
                "command_manifest_sha256": _sha(_json_bytes(manifest)),
                "flags": flags,
            },
        )
        self.memory_schema = {
            "id": schema.get("$id"),
            "sha256": _sha(_json_bytes(schema)),
            "evidence_ref": discovery,
        }
        self.capabilities = {"memory-fact": {"available": True, "evidence_ref": discovery}}
        reasons = {
            "skill": "command-or-required-output-absent",
            "schedule": "command-or-required-output-absent",
            "instruction": "closed-memory-schema-omits-kind"
            if "instruction" not in (enum or [])
            else "command-or-required-output-absent",
        }
        for capability in ("skill", "instruction", "schedule"):
            if flags[capability]:
                self.capabilities[capability] = {"available": True, "evidence_ref": discovery}
            else:
                ref = self.add(
                    "capability-absence",
                    {
                        "capability": capability,
                        "exit_code": 0,
                        "binary": dict(self.binary),
                        "reason": reasons[capability],
                    },
                )
                self.capabilities[capability] = {"available": False, "evidence_ref": ref}

    def available(self, kind: str) -> bool:
        return bool(self.capabilities.get(kind, {}).get("available"))

    def preflight_provider(self) -> None:
        if self.provider is None:
            raise _Stop("CURIE_CREDENTIALS is not set; no paid proof can run")
        self.prices = {
            model: {k: Decimal(str(v)) for k, v in pricing.items() if k in ("prompt", "completion")}
            for model, pricing in self.provider.catalog().items()
        }
        remaining = self.provider.remaining()
        budget = self.o.budget if remaining is None else min(self.o.budget, remaining)
        if budget <= SHUTDOWN_RESERVE:
            raise _Stop("known provider credit cannot cover the campaign reserve")
        self.ledger = _Ledger(self.o.output_dir / "ledger.sqlite", budget, self.provider)

    def overshoot(self) -> Decimal:
        """One final generation at the native output cap, priced from the catalog."""

        price = self.prices[ACTOR_MODEL]
        bound = (
            price["prompt"] * ACTOR_PROMPT_TOKENS_BOUND + price["completion"] * AGENT_OUTPUT_TOKENS
        )
        return (bound * RESERVE_MARGIN).quantize(Decimal("0.000001"))

    def actor_reserve(self) -> Decimal:
        """The full native per-session cap plus one final-generation overshoot."""

        return AGENT_DAILY_USD + self.overshoot()

    # Scheduled-fire reservations ---------------------------------------------------
    #
    # The worker cron loop fires an agent one-shot at most once: a deferred slot is
    # re-queued only before its turn ran, and a lapsed claim is recorded
    # `reclaimed` and never republished (cron_loop.py). So one reservation per
    # schedule a message can create, with no retry reservation.

    def reserve_fire(self, scope: Mapping[str, Any], why: str) -> None:
        assert self.ledger is not None
        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        operation = self.ledger.admit("schedule", self.actor_reserve(), {**bare, "why": why})
        self.unbound_fires.setdefault(str(scope["agent_id"]), []).append(operation)
        self.open_fires.add(operation)

    def bind_fire(self, scope: Mapping[str, Any], schedule_id: str) -> str | None:
        agent = str(scope["agent_id"])
        if (agent, schedule_id) in self.bound_fires:
            return self.bound_fires[(agent, schedule_id)]
        unbound = self.unbound_fires.get(agent, [])
        if not unbound:
            return None
        operation = unbound.pop(0)
        self.bound_fires[(agent, schedule_id)] = operation
        self.reservation_row(scope, schedule_id, "reserved")
        return operation

    def release_fire(self, operation: str | None, reason: str) -> None:
        assert self.ledger is not None
        if operation is not None and operation in self.open_fires:
            self.ledger.release(operation, reason)
            self.open_fires.discard(operation)

    def reconcile_fires(self, scope: Mapping[str, Any]) -> None:
        """At scenario end, release reservations for schedules a message did not
        create (the CLI shows no unbound active schedule) and bind the rest."""

        agent = str(scope["agent_id"])
        if not self.unbound_fires.get(agent):
            return
        bound = {sid for (owner, sid) in self.bound_fires if owner == agent}
        active = [
            str(r["id"])
            for r in self.snapshot(scope)[0].get("schedules", [])
            if r.get("state") == "active" and str(r["id"]) not in bound
        ]
        for schedule_id in active:
            self.bind_fire(scope, schedule_id)
        for operation in self.unbound_fires.pop(agent, []):
            self.release_fire(operation, "the CLI shows no schedule created for it")

    def close_fires(self, reason: str) -> None:
        """A stack going down can no longer fire; an unobserved fire stays charged."""

        assert self.ledger is not None
        for operation in sorted(self.open_fires):
            self.ledger.hold_unresolved(operation, reason)
        self.open_fires.clear()
        self.unbound_fires.clear()

    def judge_reserve(self, prompt: str) -> Decimal:
        price = self.prices[JUDGE_MODEL]
        tokens = len(prompt) // JUDGE_PROMPT_CHARS_PER_TOKEN + 1
        bound = price["prompt"] * tokens + price["completion"] * JUDGE_MAX_TOKENS
        return max(Decimal("0.001"), (bound * RESERVE_MARGIN).quantize(Decimal("0.000001")))

    def judge(self, prompt: str, purpose: str, scope: Mapping[str, Any]) -> str:
        assert self.ledger is not None and self.provider is not None
        self.call_sequence += 1
        sequence = self.call_sequence
        try:
            operation = self.ledger.admit(purpose, self.judge_reserve(prompt), scope)
            self.paid_started = True
        except _Stop as exc:
            self.stopped = str(exc)
            raise
        before = self.ledger.last_usage
        try:
            response = self.provider.complete(prompt)
        except _Stop as exc:
            self.ledger.fail(operation, "judge charge status is unknown")
            self.stopped = str(exc)
            raise
        settled = self.ledger.settle(operation, before, response["cost"], generation=response["id"])
        text = str(response["text"])
        self.generations.append(
            {
                "id": response["id"],
                "model": JUDGE_MODEL,
                "reported_model": response["reported_model"],
                "usage": response["usage"],
                "cost_usd": settled,
                "purpose": purpose,
                "prompt_sha256": _sha(prompt.encode()),
                "call_sequence": sequence,
                "completion_sha256": _sha(text.encode()),
                "ledger_operation_id": operation,
                **scope,
            }
        )
        self.last_verdict = (prompt, text, response["id"], operation, sequence)
        return text

    def calibrate(self) -> None:
        from .learning_judge import JudgeSpec, LearningJudgeScorer, parse_judge_verdict
        from .models import EvalCase, Grader, GraderKind

        examples = self.calibration_doc["examples"]
        specs = {
            str(e["id"]): JudgeSpec(
                rubric=str(e["rubric"]),
                evidence=e["evidence"],
                deterministic=ScoreResult(
                    passed=bool(e["deterministic"]["passed"]),
                    detail=e["deterministic"].get("detail"),
                ),
            )
            for e in examples
        }
        scorer = LearningJudgeScorer(
            specs, complete=lambda prompt: self.judge(prompt, "calibration", {})
        )
        for example in examples:
            self.last_verdict = None
            case = EvalCase(
                id=str(example["id"]),
                input=str(example["input"]),
                grader=Grader(kind=GraderKind.CONTAINS, expected="unused"),
            )
            evidence = example["evidence"]
            scorer.score(case, str(evidence["output"]), list(evidence["trajectory"]))
            if self.stopped:
                raise _Stop(self.stopped)
            if self.last_verdict is None:
                raise _Stop(f"calibration {example['id']} never reached the judge")
            prompt, text, generation, operation, sequence = self.last_verdict
            verdict: str
            try:
                verdict = parse_judge_verdict(text).verdict
            except ValueError:
                verdict = "MALFORMED"
            self.calibration["rows"].append(
                {
                    "id": example["id"],
                    "group": example["group"],
                    "expected": example["expected"],
                    "verdict": verdict,
                    "evidence": copy.deepcopy(evidence),
                    "deterministic": copy.deepcopy(example["deterministic"]),
                    "prompt": prompt,
                    "prompt_sha256": _sha(prompt.encode()),
                    "call_sequence": sequence,
                    "completion": text,
                    "completion_sha256": _sha(text.encode()),
                    "generation_id": generation,
                    "ledger_operation_id": operation,
                    "model": JUDGE_MODEL,
                    "usage": self.generations[-1]["usage"],
                    "source": "provider",
                }
            )
            if verdict != example["expected"]:
                self.failures.append(f"calibration {example['id']} judged {verdict}")
        by_group = {str(e["group"]): e for e in examples}
        for control in self.calibration_doc.get("deterministic_controls", []):
            called: list[str] = []
            group = str(control["group"])
            example = by_group[group]
            spec = JudgeSpec(
                rubric=str(example["rubric"]),
                evidence=example["evidence"],
                deterministic=ScoreResult(
                    passed=False, detail=str(control["deterministic"]["detail"])
                ),
            )

            def forbidden(prompt: str, sink: list[str] = called) -> str:
                sink.append(prompt)
                return ""

            control_scorer = LearningJudgeScorer({group: spec}, complete=forbidden)
            result = control_scorer.score(
                EvalCase(
                    id=group,
                    input="control",
                    grader=Grader(kind=GraderKind.CONTAINS, expected="unused"),
                ),
                str(example["evidence"]["output"]),
                list(example["evidence"]["trajectory"]),
            )
            self.calibration["deterministic_controls"].append(
                {
                    "id": group,
                    "deterministic": "FAIL",
                    "judge_called": bool(called),
                    "verdict": "PASS" if result.passed else "FAIL",
                }
            )
            if called or result.passed:
                self.failures.append(f"deterministic control {group} reached the judge")
        if self.failures:
            raise _Stop("calibration failed; scored scenarios were not executed")

    # Stack lifecycle ------------------------------------------------------------

    def compose_extra(self) -> dict[str, Any]:
        assert self.stack is not None
        return {
            "compose_files": [str(self.base_compose), str(self.stack.override)],
            "compose_project": self.stack.project,
            "compose_disable_env_file": True,
        }

    def up(self, repetition: int) -> None:
        for attempt in (1, 2):
            self.stack = _new_stack(
                self.o.output_dir, self.run_id, repetition, self.stub_port, attempt
            )
            self.platform_keys.append(self.stack.api_key)
            self.stack.write_override()
            assert self.stack is not None
            code, _, _, _, stderr = self.cli(
                [
                    "local",
                    "up",
                    "--build",
                    "--model",
                    ACTOR_MODEL,
                    "--project",
                    self.stack.project,
                    "-f",
                    str(self.base_compose),
                    "-f",
                    str(self.stack.override),
                ],
                {"repetition": repetition},
                credentials=True,
                timeout=5400,
                extra=self.compose_extra(),
            )
            self.ownership.observe(self.stack)
            if code == 0:
                self.verify_started()
                return
            # Resource setup failed: tear down this partial owned stack by its
            # recorded identity. Retry once with a fresh project and fresh
            # ports, but never once any paid call has started.
            self.down(repetition)
            with contextlib.suppress(_Invalid):
                self.ownership.remove_leftovers(images=False)
            if attempt == 2 or self.paid_started:
                raise _Stop(f"local up failed:\n{stderr}")
            self.setup_retries.append({"repetition": repetition, "reason": stderr[-2000:]})

    def verify_started(self) -> None:
        assert self.stack is not None
        for identity in list(self.ownership.containers):
            lines = _docker_lines(
                "container", "inspect", "--format", "{{json .NetworkSettings.Ports}}", identity
            )
            for line in lines:
                ports = json.loads(line) if line.strip() not in ("", "null") else {}
                for bindings in (ports or {}).values():
                    for binding in bindings or []:
                        if binding.get("HostIp") not in ("127.0.0.1", ""):
                            raise _Stop("a started container published beyond loopback")
        self.worker = {"max_attempts": self.worker_max_attempts()}
        if self.worker["max_attempts"] != 1:
            raise _Stop("the private worker does not run ordinary turns with one attempt")

    def worker_max_attempts(self) -> int | None:
        """Read CURIE_MAX_ATTEMPTS from the started worker container's configuration,
        keeping only that one value (nothing credential-bearing leaves the inspect)."""

        assert self.stack is not None
        for identity in list(self.ownership.containers):
            service = _docker_lines(
                "container",
                "inspect",
                "--format",
                '{{index .Config.Labels "com.docker.compose.service"}}',
                identity,
            )
            if service != ["curie-worker"]:
                continue
            lines = _docker_lines(
                "container", "inspect", "--format", "{{json .Config.Env}}", identity
            )
            for item in json.loads(lines[0]) if lines else []:
                name, _, value = str(item).partition("=")
                if name == "CURIE_MAX_ATTEMPTS":
                    return int(value) if value.isdigit() else None
            return None
        return None

    def down(self, repetition: int) -> None:
        if self.stack is None:
            return
        with contextlib.suppress(_Invalid):
            self.ownership.observe(self.stack)
        code, _, _, _, stderr = self.cli(
            [
                "local",
                "down",
                "--wipe",
                "--yes",
                "--project",
                self.stack.project,
                "-f",
                str(self.base_compose),
                "-f",
                str(self.stack.override),
            ],
            {"repetition": repetition},
            timeout=900,
            extra=self.compose_extra(),
        )
        if code != 0:
            self.failures.append(f"local down failed: {stderr}")
        if self.ledger is not None:
            self.close_fires("stack torn down before the fire was observed")
        self.stack = None

    def resolver_preflight(self) -> None:
        """Run the candidate runner image's own resolver; keep only host/model/booleans."""

        assert self.stack is not None
        image = f"ghcr.io/curie-eng/curie-runner:{self.stack.tag}"
        script = (
            "import json,os\nfrom urllib.parse import urlsplit\n"
            "from curie_runner.sdk_auth import resolve_sdk_env\n"
            "env=dict(os.environ)\nresolve_sdk_env(env)\n"
            "u=urlsplit(env.get('ANTHROPIC_BASE_URL') or '')\n"
            "print(json.dumps({'host':u.hostname,'scheme':u.scheme,"
            "'userinfo':u.username is not None or u.password is not None,"
            "'model':env.get('CURIE_MODEL'),'auth_token':bool(env.get('ANTHROPIC_AUTH_TOKEN')),"
            "'api_key':bool(env.get('ANTHROPIC_API_KEY'))}))\n"
        )
        env = _docker_env()
        env["CURIE_CREDENTIALS"] = self.key or ""
        docker = shutil.which("docker") or "docker"
        created = subprocess.run(
            [
                docker,
                "create",
                "--label",
                f"{OWNER_LABEL}={self.ownership.project_prefix}",
                "--entrypoint",
                "python",
                "-e",
                "CURIE_CREDENTIALS",
                "-e",
                f"CURIE_MODEL={ACTOR_MODEL}",
                "-e",
                "CURIE_FAKE_MODEL=0",
                image,
                "-c",
                script,
            ],
            env=env,
            capture_output=True,
            timeout=300,
            check=False,
            stdin=subprocess.DEVNULL,
        )
        if created.returncode != 0:
            raise _Stop("the resolver preflight container could not be created")
        identity = created.stdout.decode().strip().splitlines()[-1]
        self.ownership.resolvers[identity] = self.ownership.project_prefix
        try:
            started = subprocess.run(
                [docker, "start", "--attach", identity],
                env=_docker_env(),
                capture_output=True,
                timeout=300,
                check=False,
                stdin=subprocess.DEVNULL,
            )
        finally:
            self.ownership.removals.append({"kind": "resolver_containers", "id": identity})
            subprocess.run(
                [docker, "rm", "--force", identity],
                env=_docker_env(),
                capture_output=True,
                timeout=120,
                check=False,
            )
        lines = started.stdout.decode("utf-8", errors="replace").strip().splitlines()
        try:
            resolved = _obj(json.loads(lines[-1]), "resolver output")
        except (IndexError, ValueError) as exc:
            raise _Stop("the resolver preflight produced no sanitized result") from exc
        self.resolved = {
            key: resolved.get(key)
            for key in ("host", "scheme", "userinfo", "model", "auth_token", "api_key")
        }
        if (
            resolved.get("host") != PROVIDER_HOST
            or resolved.get("scheme") != "https"
            or resolved.get("userinfo") is not False
            or resolved.get("model") != ACTOR_MODEL
        ):
            raise _Stop("the resolved model route is not the pinned OpenRouter actor")

    # Agents, snapshots, messages and transcripts ---------------------------------

    def provision(self, scope: dict[str, Any]) -> str:
        assert self.stack is not None
        name = str(scope["agent_id"])
        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        deployed = self.must(
            [
                "local",
                "deploy",
                "--agent",
                name,
                "--plugin-dir",
                str(FIXTURES / "learning-bundle"),
                "--slack-channel",
                self.channels[0],
            ],
            bare,
            timeout=600,
        )
        agent_id = _text(_obj(_obj(deployed, "deploy").get("agent"), "agent").get("id"), "id")
        status, _ = self.api(
            "POST", f"/agents/{agent_id}/channels", {"kind": "slack", "address": self.channels[1]}
        )
        if status not in (200, 201):
            raise _Stop(f"binding the second channel answered {status}")
        flags = ["--model", ACTOR_MODEL, "--memory-writes", "on"]
        if self.available("skill"):
            flags += ["--learned-skills", "on"]
        if self.available("schedule"):
            flags += ["--agent-schedules", "on"]
        if self.available("instruction"):
            flags += ["--agent-wide-learning", "on"]
        readback = _obj(self.must(["local", "overrides", name, *flags], bare), "overrides")
        expected = {"memory_writes": True, "model": ACTOR_MODEL}
        if self.available("skill"):
            expected["learned_skills"] = True
        if self.available("schedule"):
            expected["agent_schedules"] = True
        if self.available("instruction"):
            expected["agent_wide_learning"] = True
        for key, value in expected.items():
            if readback.get(key) != value:
                raise _Stop(f"override {key} did not read back as requested")
        caps = _obj(
            self.must(
                [
                    "local",
                    "budget",
                    name,
                    "--limit",
                    str(AGENT_DAILY_USD),
                    "--output-tokens",
                    str(AGENT_OUTPUT_TOKENS),
                ],
                bare,
            ),
            "budget",
        )
        cap = caps.get("max_usd_per_day")
        if (
            not isinstance(cap, (int, float, str))
            or Decimal(str(cap)) != AGENT_DAILY_USD
            or caps.get("max_output_tokens_per_run") != AGENT_OUTPUT_TOKENS
        ):
            raise _Stop(f"native agent budget did not read back: {json.dumps(caps)}")
        self.agent_caps[name] = {
            "max_usd_per_day": str(cap),
            "max_output_tokens_per_run": AGENT_OUTPUT_TOKENS,
            "model_override": readback.get("model"),
        }
        guidance = self.o.output_dir / "guidance.txt"
        applied = _applied_guidance(str(self.guidance["text"]))
        guidance.write_text(applied, encoding="utf-8")
        self.must(["local", "memory", name, "--guidance-from", str(guidance)], bare)
        shown = _obj(self.must(["local", "memory", name, "--guidance"], bare), "guidance")
        if shown.get("text") != applied or shown.get("source") != "operator":
            raise _Stop("the fixed operator memory guidance did not read back")
        return agent_id

    def snapshot(self, scope: Mapping[str, Any]) -> tuple[dict[str, Any], Any]:
        from .learning_metrics import EvidenceError, LearningClass, parse_cli_snapshot

        name = str(scope["agent_id"])
        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        skills = (
            self.must(["local", "skills", name, "list"], bare) if self.available("skill") else None
        )
        memory = _obj(self.must(["local", "memory", name], bare), "memory")
        facts = {str(f["id"]): f for f in _lst(memory.get("facts"), "facts")}
        for channel in self.channels:
            scoped = _obj(
                self.must(["local", "memory", name, "--channel", f"slack={channel}"], bare),
                "memory",
            )
            for fact in _lst(scoped.get("facts"), "facts"):
                facts.setdefault(str(fact["id"]), fact)
        if self.available("instruction"):
            for fact in facts.values():
                missing = MEMORY_ROW_FIELDS - set(_obj(fact, "memory row"))
                if missing:
                    raise _Stop(f"a memory row lacks required fields {sorted(missing)}")
        merged = {
            "agent": memory.get("agent"),
            "entries": memory.get("entries", []),
            "facts": list(facts.values()),
        }
        schedules = (
            self.must(["local", "schedules", "--agent", name], bare)
            if self.available("schedule")
            else None
        )
        available = frozenset(LearningClass(k) for k in DURABLE if self.available(k))
        try:
            snap = parse_cli_snapshot(
                agent=name, skills=skills, memory=merged, schedules=schedules, available=available
            )
        except EvidenceError as exc:
            raise _Stop(f"a pinned CLI observation is malformed: {exc}") from exc
        state = {
            "skills": [dict(r) for r in snap.skills],
            "facts": [dict(r) for r in snap.facts],
            "schedules": [dict(r) for r in snap.schedules],
        }
        return state, snap

    def new_thread(self) -> str:
        while True:
            thread = f"{int(time.time())}.{secrets.randbelow(1_000_000):06d}"
            if thread not in self.threads:
                self.threads.add(thread)
                return thread

    def stub_port_free(self) -> bool:
        """Probe port B on the host and interface the message child's stub binds."""

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                probe.bind((_stub_bind_host(), self.stub_port))
            except OSError:
                return False
        return True

    def wait_for_stub_port(self) -> None:
        deadline = time.monotonic() + STUB_PORT_WAIT_SECONDS
        while not self.stub_port_free() and time.monotonic() < deadline:
            time.sleep(STUB_PORT_POLL_SECONDS)

    def message(
        self, scope: Mapping[str, Any], text: str, channel: str, thread: str
    ) -> dict[str, Any]:
        assert self.ledger is not None
        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        if thread.startswith(EVAL_ISOLATE_PREFIX):
            raise _Stop("an ordinary thread must not use the eval isolation prefix")
        argv = [
            "local",
            "message",
            text,
            "--agent",
            str(scope["agent_id"]),
            "--channel",
            channel,
            "--thread",
            thread,
            "--timeout-secs",
            "300",
        ]
        for attempt in (1, 2):
            self.wait_for_stub_port()
            operation = self.ledger.admit("actor", self.actor_reserve(), bare)
            self.paid_started = True
            before = self.ledger.last_usage
            self.live_children += 1
            try:
                code, parsed, invocation, result_ref, stderr = self.cli(
                    argv, bare, cwd=self.o.output_dir, timeout=420
                )
            finally:
                self.live_children -= 1
            said = stderr
            if isinstance(parsed, dict) and parsed.get("error") is not None:
                said += json.dumps(parsed["error"])
            if code == 0 or STUB_BIND_ERROR not in said:
                break
            # The child failed binding its stub on B before it enqueued anything:
            # a proven pre-dispatch failure, so no paid work exists to charge.
            self.ledger.release(operation, "the message child could not bind the Slack stub")
            if attempt == 2:
                # Only an unrecovered failure (the retry failed too) is a bind error.
                self.bind_errors += 1
                self.failures.append(
                    f"{scope['case_id']} ERROR: the message child could not bind the Slack "
                    "stub on port B twice"
                )
                return {
                    "invocation_id": invocation,
                    "result_ref": result_ref,
                    "exit_code": code,
                    "reply": "",
                    "thread": thread,
                    "channel": channel,
                    "finalized": False,
                    "bind_failed": True,
                    "generation": None,
                }
        if attempt == 2:
            self.recovered_bind_retries += 1
        # The provider gives the harness no per-turn generation id for runner
        # traffic, so the row is labeled by its configured route, not an invented id.
        settled = self.ledger.settle_stable(operation, before, record=operation)
        self.generations.append(
            {
                "id": operation,
                "generation_id": None,
                "identity_basis": "configured-route",
                "route": {
                    "resolved_host": self.resolved.get("host"),
                    "resolved_model": self.resolved.get("model"),
                    "agent_model_override": self.agent_caps.get(str(scope["agent_id"]), {}).get(
                        "model_override"
                    ),
                    "worker_model": ACTOR_MODEL,
                },
                "model": ACTOR_MODEL,
                "usage": {"source": "shared-key-delta"},
                "cost_usd": settled,
                "purpose": "actor",
                "ledger_operation_id": operation,
                **bare,
            }
        )
        result = parsed if isinstance(parsed, dict) else {}
        if code != 0 or result.get("finalized") is not True:
            self.failures.append(f"{scope['case_id']} turn did not finalize: {stderr[-300:]}")
        self.sweep_schedules(scope)
        return {
            "invocation_id": invocation,
            "result_ref": result_ref,
            "exit_code": code,
            "reply": str(result.get("reply") or ""),
            "thread": thread,
            "channel": channel,
            "finalized": result.get("finalized") is True,
            "generation": operation,
        }

    def sweep_schedules(self, scope: Mapping[str, Any]) -> None:
        """After every actor turn, no active schedule may stay unreserved: bind it to a
        pending reservation, else reserve it if that fits, else cancel it at once
        through the operator CLI and record an off-target write and a quiesce action."""

        if not self.available("schedule"):
            return
        assert self.ledger is not None
        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        agent = str(scope["agent_id"])
        listed = _obj(self.must(["local", "schedules", "--agent", agent], bare), "schedules")
        for raw in _lst(listed.get("agent_schedules"), "agent_schedules"):
            row = _obj(raw, "schedule")
            schedule_id = str(row.get("id"))
            if row.get("state") != "active" or (agent, schedule_id) in self.bound_fires:
                continue
            if self.bind_fire(scope, schedule_id) is not None:
                continue
            reserve = self.actor_reserve()
            if self.ledger.fits(reserve):
                operation = self.ledger.admit(
                    "schedule", reserve, {**bare, "why": "unexpected schedule"}
                )
                self.open_fires.add(operation)
                self.bound_fires[(agent, schedule_id)] = operation
                self.reservation_row(scope, schedule_id, "reserved")
                continue
            self.must(["local", "schedules", "--agent", agent, "--cancel", schedule_id], bare)
            self.reservation_row(scope, schedule_id, "cancelled")
            self.quiesce_actions.append(
                {**bare, "schedule_id": schedule_id, "action": "operator-cancel"}
            )
            self.failures.append(
                f"{scope['case_id']} off-target schedule {schedule_id} had no reservation "
                "room and was cancelled"
            )

    def reservation_row(self, scope: Mapping[str, Any], schedule_id: str, disposition: str) -> None:
        self.schedule_reservations.append(
            {
                "repetition": scope["repetition"],
                "case_id": scope["case_id"],
                "schedule_id": schedule_id,
                "disposition": disposition,
            }
        )

    def transcript(
        self, scope: Mapping[str, Any], agent_id: str, channel: str, thread: str, text: str
    ) -> tuple[dict[str, Any], list[dict[str, Any]]]:
        """Return the exact runner turn item written for the message ``text``.

        The evidence is the last turn item whose user message carries the sent
        text, never a positional slice. Malformed items reject the transcript."""

        # The worker stores transcripts agent-wide under the conversation id
        # (binding.py history_ref); a local message turn's id is slack:<channel>:<thread>.
        thread_key = f"slack:{channel}:{thread}"
        path = f"/agents/{agent_id}/state/transcript/{urllib.parse.quote(thread_key, safe='')}"
        sent = text.strip()
        deadline = time.monotonic() + 30
        while True:
            status, entry = self.api("GET", path)
            if status not in (200, 404) or (status == 404 and time.monotonic() > deadline):
                raise _Stop(f"no transcript for thread {thread} (status {status})")
            if status == 200:
                record = _obj(entry, "transcript entry")
                if record.get("key") != thread_key:
                    raise _Stop("the transcript entry is bound to another thread")
                value = record.get("value")
                if not isinstance(value, list):
                    raise _Stop("the transcript value is not the runner's JSON array")
                # history.py TurnRecord.to_dict: {"type": "turn", "user", "ts", "messages"}
                matches: list[tuple[int, dict[str, Any]]] = []
                for index, item in enumerate(value):
                    if not isinstance(item, dict) or not isinstance(item.get("type"), str):
                        raise _Stop(f"transcript item {index} is malformed")
                    if item["type"] != "turn":
                        continue
                    user, messages = item.get("user"), item.get("messages")
                    if (
                        not isinstance(user, str)
                        or not isinstance(messages, list)
                        or any(
                            not isinstance(m, dict) or not isinstance(m.get("role"), str)
                            for m in messages
                        )
                    ):
                        raise _Stop(f"transcript turn item {index} is malformed")
                    if sent and sent in user:
                        matches.append((index, item))
                if matches:
                    break
            if time.monotonic() > deadline:
                raise _Stop(f"no transcript turn for the message sent on thread {thread}")
            time.sleep(3)
        index, item = matches[-1]
        turn: list[dict[str, Any]] = list(item["messages"])
        return {
            "binding_key": thread_key,
            "state_caller": "platform",
            "turn_index": index,
            "turn_ts": item.get("ts"),
            "turn_user": _bounded(str(item["user"])),
            "messages": turn,
        }, turn

    # Scoring --------------------------------------------------------------------

    def admit_predicates(
        self,
        predicates: Sequence[Mapping[str, Any]],
        before: Mapping[str, Any],
        after: Mapping[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        available = {c for c in DURABLE if self.available(c)}
        return _admit_predicates(predicates, before, after, available)

    def score_decision(
        self,
        scope: Mapping[str, Any],
        fixture: Mapping[str, Any],
        spec: Mapping[str, Any],
        before: tuple[dict[str, Any], Any],
        after: tuple[dict[str, Any], Any],
        sent: Mapping[str, Any],
        transcript: Mapping[str, Any],
        turn: list[dict[str, Any]],
    ) -> None:
        from .learning_judge import JudgeSpec, LearningJudgeScorer, parse_judge_verdict
        from .learning_metrics import EvidenceError, match_tool_results, score_state
        from .models import EvalCase, Grader, GraderKind

        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        expected = str(spec["expected"])
        changed = sorted(_state_diff(before[0], after[0]))
        durable = sorted({kind for kind, _ in changed})
        mutations = [
            {"kind": kind, "item_id": item, "operation": "change"} for kind, item in changed
        ]
        try:
            attempts_all = match_tool_results(turn)
            attempts = [
                {"call_id": a.call_id, "name": a.name, "matched": a.matched, "is_error": a.is_error}
                for a in attempts_all
                if any(t in a.name.lower() for t in _PERSISTENCE_TOOLS)
            ]
            transcript_error = None
        except EvidenceError as exc:
            attempts, transcript_error = [], str(exc)
        if "error" in transcript:
            transcript_error = str(transcript["error"])
        kept, unavailable = self.admit_predicates(spec["predicates"], before[0], after[0])
        status = "FAIL"
        detail = ""
        deterministic = False
        semantic = "NOT_RUN"
        action: str | None = None
        predicted: list[str] = list(durable)
        judge_ref: dict[str, Any] | None = None
        request = _decision_request(fixture, spec)
        if expected in DURABLE and not self.available(expected):
            status, detail = "NOT_AVAILABLE", "expected capability is absent"
        elif sent.get("bind_failed"):
            status, detail = "ERROR", "the message child could not bind the Slack stub on port B"
        elif not kept:
            status, detail = "NOT_AVAILABLE", "every predicate needs an absent field"
        elif transcript_error is not None:
            status, detail = "ERROR", f"transcript evidence: {transcript_error}"
        elif _forbids_persistence(spec) and attempts:
            # Any persistence attempt (success, error or unmatched) fails a
            # forbidden decision deterministically, before any judging.
            status, detail = "FAIL", "a persistence attempt was made where none is allowed"
        elif durable and not any(a["matched"] and not a["is_error"] for a in attempts):
            status, detail = "ERROR", "missing tool evidence"
        else:
            try:
                result = score_state(before[1], after[1], {"predicates": kept})
            except EvidenceError as exc:
                result = ScoreResult(passed=False, detail=f"evidence error: {exc}")
                status = "ERROR"
            deterministic = result.passed
            output = str(sent["reply"])
            trajectory = [str(a["name"]) for a in attempts]
            evidence = {
                "output": output,
                "trajectory": trajectory,
                "refs": {
                    "reply:final": output,
                    "state:mutations": mutations,
                    "state:after": {k: v for k, v in after[0].items() if v},
                    "request:teaching": request,
                    "expected:meaning": _expected_meaning(spec),
                },
            }
            key = f"{scope['case_id']}:{spec['id']}"
            scorer = LearningJudgeScorer(
                {
                    key: JudgeSpec(
                        rubric=_decision_rubric(spec),
                        evidence=evidence,
                        deterministic=result,
                    )
                },
                complete=lambda prompt: self.judge(prompt, "judge", bare),
            )
            self.last_verdict = None
            judged = scorer.score(
                EvalCase(
                    id=key,
                    input=request,
                    grader=Grader(kind=GraderKind.CONTAINS, expected="unused"),
                ),
                output,
                trajectory,
            )
            if self.stopped:
                raise _Stop(self.stopped)
            if self.last_verdict is not None:
                prompt_text, completion, generation_id, _, sequence = self.last_verdict
                judge_ref = {
                    "generation_id": generation_id,
                    "case_id": scope["case_id"],
                    "decision_id": spec["id"],
                    "evidence": copy.deepcopy(evidence),
                    "call_sequence": sequence,
                    "prompt_sha256": _sha(prompt_text.encode()),
                    "completion_sha256": _sha(completion.encode()),
                    "completion": completion,
                }
                try:
                    verdict = parse_judge_verdict(completion)
                    semantic = verdict.verdict
                    action = None if verdict.action is None else str(verdict.action)
                except ValueError:
                    semantic = "ERROR"
            if not durable and semantic == "PASS" and action in ("clarify", "nothing"):
                predicted = [action]
            if status != "ERROR":
                status = (
                    "PASS"
                    if (
                        judged.passed
                        and deterministic
                        and expected in predicted
                        and semantic == "PASS"
                    )
                    else "FAIL"
                )
            detail = result.detail or judged.detail or ""
        if status == "NOT_AVAILABLE":
            predicted = list(durable)
        duplicates = 0
        if fixture.get("correction_duplicates") is not None and spec["id"] == "correct":
            subject = str(fixture.get("subject", ""))
            heads = [r for r in after[0]["skills"] if r.get("slug") == subject]
            duplicates = max(0, len(heads) - 1)
        observation = self.add(
            "teaching-observation",
            {
                **bare,
                "decision_id": spec["id"],
                "before": before[0],
                "after": after[0],
                "transcript": dict(transcript),
                "output": sent["reply"],
                "semantic_action": action,
                "deterministic": deterministic,
                "semantic": semantic,
                "correction_duplicates": duplicates,
                "unavailable_predicates": unavailable,
                "admitted_predicates": kept,
                "available": sorted(c for c in DURABLE if self.available(c)),
                "substitutions": copy.deepcopy(self.substitutions),
                "invocation_id": sent["invocation_id"],
            },
        )
        self.decisions.append(
            {
                **bare,
                "fixture_id": fixture["id"],
                "decision_id": spec["id"],
                "expected": expected,
                "predicted": predicted,
                "status": status,
                "mutations": mutations,
                "attempts": attempts,
                "distractor": bool(spec.get("distractor", False)),
                "correction_duplicates": duplicates,
                "evidence_ref": observation,
                "detail": _bounded(detail),
            }
        )
        if judge_ref is not None:
            self.decisions[-1]["judge_ref"] = judge_ref
        if status in ("FAIL", "ERROR"):
            self.failures.append(f"{scope['case_id']} {spec['id']} {status}: {_bounded(detail)}")

    # Scenarios ------------------------------------------------------------------

    def teach(
        self,
        scope: dict[str, Any],
        fixture: Mapping[str, Any],
        agent_id: str,
        thread: str,
        index: int,
    ) -> tuple[dict[str, Any], tuple[dict[str, Any], Any]]:
        before = self.snapshot(scope)
        text = str(fixture["messages"][index])
        if self.available("schedule") and any(
            int(spec["message"]) == index and spec["expected"] == "schedule"
            for spec in fixture["decisions"]
        ):
            self.reserve_fire(scope, f"message {index} expects a schedule")
        sent = self.message(scope, text, self.channels[0], thread)
        after = self.snapshot(scope)
        try:
            transcript, turn = self.transcript(scope, agent_id, self.channels[0], thread, text)
        except _Stop as exc:
            transcript, turn = {"error": str(exc)}, []
            self.failures.append(f"{scope['case_id']} transcript: {exc}")
        for spec in fixture["decisions"]:
            if int(spec["message"]) == index:
                self.score_decision(scope, fixture, spec, before, after, sent, transcript, turn)
        if int(fixture.get("primary_message", 0)) == index:
            self.cases.append(
                {
                    **{k: scope[k] for k in ("repetition", "case_id", "agent_id")},
                    "fixture_id": fixture["id"],
                    "primary": True,
                    "invocation_id": sent["invocation_id"],
                    "reply_ref": sent["result_ref"],
                    "transcript_ref": self.decisions[-1]["evidence_ref"],
                    "actor_generation_id": None,
                    "actor_record_id": sent["generation"],
                    "stub_port": self.stub_port,
                    "stub_bind_succeeded": sent["exit_code"] == 0,
                }
            )
        return sent, after

    def consent(self, scope: dict[str, Any], fixture: Mapping[str, Any], agent_id: str) -> None:
        """Run the fixture's consent conversation (offer, refusal, approval) in its own
        thread as three labeled decisions; the first two forbid persistence attempts."""

        specs = [spec for spec in _fixture_decisions(fixture) if "consent_message" in spec]
        if not specs or not all(self.available(str(k)) for k in fixture.get("requires", [])):
            return
        thread = self.new_thread()
        for spec in specs:
            text = str(spec["text"])
            before = self.snapshot(scope)
            if self.available("schedule") and spec["expected"] == "schedule":
                self.reserve_fire(scope, f"{spec['id']} expects a schedule")
            sent = self.message(scope, text, self.channels[0], thread)
            after = self.snapshot(scope)
            try:
                transcript, turn = self.transcript(scope, agent_id, self.channels[0], thread, text)
            except _Stop as exc:
                transcript, turn = {"error": str(exc)}, []
                self.failures.append(f"{scope['case_id']} {spec['id']} transcript: {exc}")
            self.score_decision(scope, fixture, spec, before, after, sent, transcript, turn)

    def scenario(self, repetition: int, raw: Mapping[str, Any]) -> None:
        family = int(str(raw["id"])[1])
        if family == 4:
            if self.f4_base is None:
                self.f4_base = datetime.now(UTC) + timedelta(minutes=F4_LEAD_MINUTES + 1)
            phrase = int(str(raw["id"])[-1])
            due = self.f4_base + timedelta(minutes=F4_STAGGER_MINUTES * (phrase - 1))
        else:
            due = datetime.now(UTC) + timedelta(minutes=SCHEDULE_LEAD_MINUTES)
        values = _scenario_values(due)
        self.channels = _scenario_channels(self.run_id, repetition, str(raw["id"]))
        literals = dict(zip(CHANNELS, self.channels, strict=True))
        fixture = _substitute(copy.deepcopy(dict(raw)), values, literals)
        self.substitutions = {"values": dict(values), "literals": literals}
        case_id = f"{fixture['id']}-R{repetition}"
        name = f"le-{self.run_id}-r{repetition}-{fixture['id'].lower()}"
        scope: dict[str, Any] = {"repetition": repetition, "case_id": case_id, "agent_id": name}
        agent_id = self.provision(scope)
        thread = self.new_thread()
        after: tuple[dict[str, Any], Any] = ({}, None)
        for index in range(len(fixture["messages"])):
            _, after = self.teach(scope, fixture, agent_id, thread, index)
        self.consent(scope, fixture, agent_id)
        control = {
            1: self.control_procedure,
            3: self.control_fact,
            4: self.control_one_shot,
            5: self.control_recurring,
            8: self.control_correction,
            2: self.control_instruction,
        }.get(family)
        if control is not None:
            control(scope, fixture, agent_id, thread, after, values)
        self.reconcile_fires(scope)

    def record_control(
        self, scope: Mapping[str, Any], control: str, status: str, detail: str
    ) -> None:
        self.controls.append(
            {
                **{k: scope[k] for k in ("repetition", "case_id", "agent_id")},
                "control": control,
                "status": status,
                "detail": detail,
            }
        )
        if status in ("FAIL", "ERROR"):
            self.failures.append(f"{scope['case_id']} control {control}: {detail}")

    def find(
        self, state: Mapping[str, Any], store: str, field_name: str, needle: str
    ) -> list[dict[str, Any]]:
        rows = state.get(store, [])
        return [r for r in rows if needle in str(r.get(field_name, ""))]

    def control_instruction(
        self,
        scope: dict[str, Any],
        fixture: Mapping[str, Any],
        agent_id: str,
        thread: str,
        after: Any,
        values: Mapping[str, str],
    ) -> None:
        if not self.available("instruction"):
            self.record_control(
                scope,
                "instruction-controls",
                "NOT_AVAILABLE",
                "instruction capability is absent; correction not exercised",
            )
            return
        for probe in fixture.get("probes", []):
            self.suffix_probe(scope, probe)
        correction = next(
            (c for c in fixture["controls"] if c.get("id") == "instruction-correction"), None
        )
        if correction is not None:
            self.instruction_correction(scope, correction, thread, after[0], values)
        conflict = next(
            (c for c in fixture["controls"] if c.get("id") == "conflicting-request"), None
        )
        if conflict is not None:
            before = self.snapshot(scope)[0]
            sent = self.message(scope, str(conflict["input"]), self.channels[0], self.new_thread())
            changed = sorted(_state_diff(before, self.snapshot(scope)[0]))
            self.record_control(
                scope,
                "conflicting-request",
                "FAIL" if changed else "PASS",
                f"durable changes {json.dumps(changed)}; reply: {_excerpt(sent['reply'])}",
            )

    def suffix_probe(self, scope: Mapping[str, Any], probe: Mapping[str, Any]) -> None:
        """A fresh thread on the probe's channel, so the reply reflects boot memory."""

        sent = self.message(scope, str(probe["input"]), str(probe["channel"]), self.new_thread())
        ok = sent["reply"].rstrip().endswith(str(probe["expected_suffix"]))
        self.record_control(
            scope,
            str(probe["id"]),
            "PASS" if ok else "FAIL",
            f"fresh-thread reply convention; reply: {_excerpt(sent['reply'])}",
        )

    def instruction_correction(
        self,
        scope: Mapping[str, Any],
        control: Mapping[str, Any],
        thread: str,
        taught: Mapping[str, Any],
        values: Mapping[str, str],
    ) -> None:
        """Correct, then undo, the taught instruction through conversation. Each step
        keeps the instruction's identity, raises its version and leaves exactly one
        active instruction carrying the expected marker and not the other one."""

        original, corrected = values["closing_marker"], values["corrected_marker"]

        def instructions(state: Mapping[str, Any]) -> list[dict[str, Any]]:
            return [
                dict(r)
                for r in state.get("facts", [])
                if r.get("kind") == "instruction" and r.get("archived") is not True
            ]

        found = [r for r in instructions(taught) if original in str(r.get("statement", ""))]
        identity = str(found[0]["id"]) if len(found) == 1 else None
        version = found[0].get("version") if len(found) == 1 else None
        probes = {int(p["after_message"]): p for p in control.get("probes", [])}
        for index, text in enumerate(control["messages"]):
            self.message(scope, str(text), self.channels[0], thread)
            rows = instructions(self.snapshot(scope)[0])
            want, gone = (corrected, original) if index == 0 else (original, corrected)
            head = next((r for r in rows if str(r["id"]) == identity), None)
            statement = "" if head is None else str(head.get("statement", ""))
            others = [
                r["id"]
                for r in rows
                if str(r["id"]) != identity
                and (
                    original in str(r.get("statement", ""))
                    or corrected in str(r.get("statement", ""))
                )
            ]
            raised = (
                head is not None
                and isinstance(version, int)
                and isinstance(head.get("version"), int)
                and head["version"] > version
            )
            ok = identity is not None and want in statement and gone not in statement
            ok = ok and raised and not others
            self.record_control(
                scope,
                f"instruction-correction-{'change' if index == 0 else 'undo'}",
                "PASS" if ok else "FAIL",
                f"identity {identity} kept {head is not None}, version {version} -> "
                f"{None if head is None else head.get('version')}, statement "
                f"{_excerpt(statement)!r}, other instructions {json.dumps(others)}",
            )
            if head is not None:
                version = head.get("version")
            if index in probes:
                self.suffix_probe(scope, probes[index])

    def control_procedure(
        self,
        scope: dict[str, Any],
        fixture: Mapping[str, Any],
        agent_id: str,
        thread: str,
        after: Any,
        values: Mapping[str, str],
    ) -> None:
        if not self.available("skill"):
            self.record_control(
                scope,
                "removed-skill",
                "NOT_AVAILABLE",
                "learned skills are absent; no item to remove",
            )
            return
        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        control = next(c for c in fixture["controls"] if c.get("id") == "removed-skill")
        heads = self.find(after[0], "skills", "slug", str(fixture["subject"]))
        if len(heads) != 1:
            self.record_control(
                scope,
                "removed-skill",
                "FAIL",
                f"no single learned skill; skills observed: "
                f"{json.dumps([(r.get('id'), r.get('slug')) for r in after[0].get('skills', [])])}",
            )
            return
        skill_id = str(heads[0]["id"])
        probes = {str(p["id"]): p for p in fixture["probes"]}
        first, second = control["probe_pair"]
        if int(scope["repetition"]) % 2 == 0:
            first, second = second, first
        deletion: str | None = None
        for arm, probe_id in (("learned", first), ("removed", second)):
            if arm == "removed":
                code, _, deletion, _, stderr = self.cli(
                    ["local", "skills", str(scope["agent_id"]), "delete", skill_id], bare
                )
                if code != 0:
                    self.record_control(scope, "removed-skill", "FAIL", f"delete: {stderr}")
                    return
            probe = probes[str(probe_id)]
            before_state = self.snapshot(scope)[0]
            sent = self.message(scope, str(probe["input"]), self.channels[0], self.new_thread())
            after_state = self.snapshot(scope)[0]
            success = str(probe["expected"]) in sent["reply"]
            ref = self.add(
                "procedure-probe",
                {
                    **bare,
                    "task_id": control["task_id"],
                    "arm": arm,
                    "skill_id": skill_id,
                    "deletion_invocation": None,
                    "pair_id": "pair-1",
                    "thread": sent["thread"],
                    "input": probe["input"],
                    "output": sent["reply"],
                    "expected_output": probe["expected"],
                    "success": success,
                    "before": {"skills": before_state["skills"]},
                    "after": {"skills": after_state["skills"]},
                    "guidance_sha256": self.guidance_sha,
                    "bundle_sha256": self.bundle_sha,
                    "invocation_id": sent["invocation_id"],
                },
            )
            self.procedure_rows.append(
                {
                    **bare,
                    "task_id": control["task_id"],
                    "arm": arm,
                    "successes": int(success),
                    "trials": 1,
                    "probe_ids": [ref],
                }
            )
        for row in self.procedure_rows[-2:]:
            for ref in row["probe_ids"]:
                self.evidence[ref]["value"]["deletion_invocation"] = deletion
        self.record_control(scope, "removed-skill", "PASS", "probes recorded; gate recomputed")

    def retention_probe(
        self, scope: Mapping[str, Any], fact_id: str, value: str, text: str, agent_id: str
    ) -> str:
        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        _, _, _, before_ref, _ = self.cli(["local", "memory", str(scope["agent_id"])], bare)
        sent = self.message(scope, text, self.channels[1], self.new_thread())
        _, parsed, _, after_ref, _ = self.cli(["local", "memory", str(scope["agent_id"])], bare)
        facts = _lst(_obj(parsed, "memory").get("facts"), "facts")
        present = any(_obj(f, "fact").get("id") == fact_id for f in facts)
        try:
            transcript: dict[str, Any] = self.transcript(
                scope, agent_id, self.channels[1], sent["thread"], text
            )[0]
        except _Stop as exc:
            transcript = {"error": str(exc)}
            self.failures.append(f"{scope['case_id']} retention transcript: {exc}")
        output = sent["reply"]
        return self.add(
            "retention-probe",
            {
                **bare,
                "fact_id": fact_id,
                "required_value": value,
                "input": text,
                "output": output,
                "thread": sent["thread"],
                "channel": self.channels[1],
                "teaching_channel": self.channels[0],
                "before_snapshot_ref": before_ref,
                "snapshot_ref": after_ref,
                "invocation_id": sent["invocation_id"],
                "result_ref": sent["result_ref"],
                "binding_key": transcript.get("binding_key", ""),
                "state_caller": "platform",
                "transcript": transcript,
                "retention_scorer": {
                    "version": 1,
                    "kind": "exact-value-and-active-fact",
                    "required_value": value,
                },
                "retention": "PASS" if present and value in output else "FAIL",
                "no_recall": value not in output,
                "no_fabrication": not NONCE_RE.search(output) and value not in output,
                "guidance_sha256": self.guidance_sha,
                "bundle_sha256": self.bundle_sha,
            },
        )

    def control_fact(
        self,
        scope: dict[str, Any],
        fixture: Mapping[str, Any],
        agent_id: str,
        thread: str,
        after: Any,
        values: Mapping[str, str],
    ) -> None:
        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        subject = str(fixture["subject"])
        recall = fixture["probes"][0]
        sent = self.message(scope, str(recall["input"]), self.channels[1], self.new_thread())
        ok = str(recall["required_value"]) in sent["reply"]
        self.record_control(
            scope,
            "cross-channel-retention",
            "PASS" if ok else "FAIL",
            f"fresh thread on the second channel; reply: {_excerpt(sent['reply'])}",
        )
        correction = next(c for c in fixture["controls"] if c.get("id") == "fact-correction")
        self.message(scope, str(correction["messages"][0]), self.channels[0], thread)
        state = self.snapshot(scope)[0]
        live = [
            f
            for f in self.find(state, "facts", "statement", subject)
            if f.get("archived") is not True
        ]
        corrected = len(live) == 1 and values["corrected_value"] in str(live[0]["statement"])
        self.record_control(
            scope,
            "fact-correction",
            "PASS" if corrected else "FAIL",
            f"{len(live)} active facts for the subject after correction: "
            f"{json.dumps([(f.get('id'), f.get('scope'), f.get('statement')) for f in live])}",
        )
        if not live:
            return
        fact_id = str(live[0]["id"])
        deleted = next(c for c in fixture["controls"] if c.get("id") == "deleted-fact")
        required = str(deleted["required_value"])
        text = str(deleted["probe"]["input"])
        before = self.retention_probe(scope, fact_id, required, text, agent_id)
        args = ["local", "memory", str(scope["agent_id"]), "--delete", fact_id]
        if live[0].get("scope") == "channel" and isinstance(live[0].get("channel"), dict):
            channel = live[0]["channel"]
            args += ["--channel", f"{channel['kind']}={channel['address']}"]
        code, _, invocation, result_ref, stderr = self.cli(args, bare)
        if code != 0:
            self.record_control(scope, "deleted-fact", "FAIL", f"delete: {stderr}")
        after_ref = self.retention_probe(scope, fact_id, required, text, agent_id)
        record = next(r for r in self.invocations if r["invocation_id"] == invocation)
        self.negative_rows.append(
            {
                **bare,
                "fact_id": fact_id,
                "deletion_invocation": {
                    "invocation_id": invocation,
                    "argv": record["argv"],
                    "exit_code": code,
                    "result_ref": result_ref,
                },
                "retention_before": self.evidence[before]["value"]["retention"],
                "retention_after": self.evidence[after_ref]["value"]["retention"],
                "before_evidence": before,
                "after_evidence": after_ref,
            }
        )

    def control_one_shot(
        self,
        scope: dict[str, Any],
        fixture: Mapping[str, Any],
        agent_id: str,
        thread: str,
        after: Any,
        values: Mapping[str, str],
    ) -> None:
        if not self.available("schedule"):
            self.record_control(
                scope, "real-clock-delivery", "NOT_AVAILABLE", "agent schedules are absent"
            )
            return
        created = _now()
        token = values["schedule_token"]
        due = self.find(after[0], "schedules", "prompt", token)
        cancel = next(c for c in fixture["controls"] if c.get("id") == "cancelled-one-shot")
        if len(due) == 1:
            self.bind_fire(scope, str(due[0]["id"]))
        self.reserve_fire(scope, "cancel-target one-shot")
        self.message(scope, str(cancel["messages"][0]), self.channels[0], thread)
        made = self.find(self.snapshot(scope)[0], "schedules", "prompt", values["cancel_token"])
        if len(made) == 1:
            self.bind_fire(scope, str(made[0]["id"]))
        self.message(scope, str(cancel["messages"][1]), self.channels[0], thread)
        if len(due) != 1 or len(made) != 1:
            self.record_control(
                scope,
                "real-clock-delivery",
                "FAIL",
                "the due or cancelled one-shot schedule was not created; observed due "
                f"{json.dumps([(r.get('id'), r.get('state'), r.get('run_at')) for r in due])}, "
                f"cancel-target {json.dumps([(r.get('id'), r.get('state')) for r in made])}",
            )
            return
        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        target, source = self.schedule_row(scope, str(made[0]["id"]))
        if source == "absent" or (target is not None and target.get("state") == "cancelled"):
            self.release_fire(
                self.bound_fires.get((str(scope["agent_id"]), str(made[0]["id"]))),
                f"schedule {made[0]['id']} observed cancelled before its slot",
            )
        for row, token_value in ((due[0], token), (made[0], values["cancel_token"])):
            operation = self.bound_fires.get((str(scope["agent_id"]), str(row["id"])))
            self.pending.append(
                {
                    "scope": bare,
                    "operation": operation,
                    "schedule_id": str(row["id"]),
                    "token": token_value,
                    "created": created,
                    "channel": self.channels[0],
                    "row": dict(row),
                }
            )

    def control_recurring(
        self,
        scope: dict[str, Any],
        fixture: Mapping[str, Any],
        agent_id: str,
        thread: str,
        after: Any,
        values: Mapping[str, str],
    ) -> None:
        if not self.available("schedule"):
            self.record_control(
                scope, "revise-and-cancel", "NOT_AVAILABLE", "agent schedules are absent"
            )
            return
        control = next(c for c in fixture["controls"] if c.get("id") == "revise-and-cancel")
        original = self.find(after[0], "schedules", "prompt", values["schedule_token"])
        if len(original) == 1:
            self.bind_fire(scope, str(original[0]["id"]))
        self.message(scope, str(control["messages"][0]), self.channels[0], thread)
        revised_state = self.snapshot(scope)[0]
        revised = [
            r
            for r in self.find(revised_state, "schedules", "prompt", values["revised_token"])
            if r.get("state") == "active"
        ]
        same = (
            len(original) == 1
            and len(revised) == 1
            and revised[0]["id"] == original[0]["id"]
            and revised[0].get("cron") == control["revised_cron"]
        )
        sent = self.message(scope, str(control["messages"][1]), self.channels[0], thread)
        final, source = (None, "absent")
        if original:
            final, source = self.schedule_row(scope, str(original[0]["id"]))
        state = None if final is None else final.get("state")
        live = [
            r
            for r in self.find(
                self.snapshot(scope)[0], "schedules", "prompt", values["revised_token"]
            )
            if r.get("state") == "active"
        ]
        cancelled = (
            bool(original)
            and not live
            and (state == "cancelled" or (final is None and source == "absent"))
        )
        if source == "error":
            self.record_control(
                scope, "revise-and-cancel", "ERROR", "the schedule read after cancel failed"
            )
            return
        if cancelled:
            self.release_fire(
                self.bound_fires.get((str(scope["agent_id"]), str(original[0]["id"]))),
                f"schedule {original[0]['id']} observed cancelled",
            )
        observed = {
            "original": [(r["id"], r.get("state"), r.get("cron")) for r in original],
            "revised": [(r["id"], r.get("state"), r.get("cron")) for r in revised],
            "after_cancel": {"state": state, "source": source, "active_matches": len(live)},
        }
        self.record_control(
            scope,
            "revise-and-cancel",
            "PASS" if same and cancelled else "FAIL",
            f"revised in place {same}, cancelled {cancelled}; observed {json.dumps(observed)}; "
            f"cancel reply: {_excerpt(sent['reply'])}",
        )

    def control_correction(
        self,
        scope: dict[str, Any],
        fixture: Mapping[str, Any],
        agent_id: str,
        thread: str,
        after: Any,
        values: Mapping[str, str],
    ) -> None:
        if not self.available("skill"):
            self.record_control(scope, "revert", "NOT_AVAILABLE", "learned skills are absent")
            return
        probes = {str(p["id"]): p for p in fixture["probes"]}
        corrected = probes["corrected-procedure"]
        sent = self.message(scope, str(corrected["input"]), self.channels[0], self.new_thread())
        ok = str(corrected["expected"]) in sent["reply"]
        self.record_control(
            scope,
            "corrected-procedure",
            "PASS" if ok else "FAIL",
            f"fresh-thread probe after correction; reply: {_excerpt(sent['reply'])}",
        )
        heads = self.find(after[0], "skills", "slug", str(fixture["subject"]))
        revert = next(c for c in fixture["controls"] if c.get("id") == "revert")
        self.message(scope, str(revert["messages"][0]), self.channels[0], thread)
        restored = self.find(self.snapshot(scope)[0], "skills", "slug", str(fixture["subject"]))
        same = (
            len(heads) == 1
            and len(restored) == 1
            and restored[0]["id"] == heads[0]["id"]
            and int(restored[0].get("revision", 0)) > int(heads[0].get("revision", 0))
        )
        self.record_control(
            scope,
            "revert",
            "PASS" if same else "FAIL",
            "same immutable skill with a new revision; before "
            f"{json.dumps([(r.get('id'), r.get('revision')) for r in heads])}, after "
            f"{json.dumps([(r.get('id'), r.get('revision')) for r in restored])}",
        )
        probe = probes["restored-procedure"]
        sent = self.message(scope, str(probe["input"]), self.channels[0], self.new_thread())
        ok = str(probe["expected"]) in sent["reply"]
        self.record_control(
            scope,
            "restored-procedure",
            "PASS" if ok else "FAIL",
            f"fresh-thread probe after revert; reply: {_excerpt(sent['reply'])}",
        )

    def schedule_row(
        self, scope: Mapping[str, Any], schedule_id: str
    ) -> tuple[dict[str, Any] | None, str]:
        """One agent schedule: from the agent_schedules list, else by its exact id.

        A cancelled or finished schedule drops out of the list, so absence from the
        list alone is not its state; `--show <id>` reads that one row.
        """

        bare = {k: scope[k] for k in ("repetition", "case_id", "agent_id")}
        name = str(scope["agent_id"])
        listed = _obj(self.must(["local", "schedules", "--agent", name], bare), "schedules")
        for raw in _lst(listed.get("agent_schedules"), "agent_schedules"):
            if str(_obj(raw, "schedule").get("id")) == schedule_id:
                return dict(raw), "list"
        code, parsed, _, _, stderr = self.cli(
            ["local", "schedules", "--agent", name, "--show", schedule_id], bare
        )
        if code == 0 and isinstance(parsed, dict):
            row = parsed if "id" in parsed else parsed.get("schedule")
            if isinstance(row, dict) and str(row.get("id")) == schedule_id:
                return dict(row), "show"
            return None, "error"
        # Only the API's documented answer for an unknown schedule counts as absent
        # (agent_schedules router: 404 "schedule not found"); any other failure
        # or malformed read is an error, never a cancellation.
        said = f"{json.dumps(parsed) if parsed is not None else ''} {stderr}"
        if code != 0 and "404" in said and "schedule not found" in said:
            return None, "absent"
        return None, "error"

    def open_window(self, repetition: int) -> datetime:
        """Bind the collector on port B once every message child of the repetition exited."""

        if self.live_children:
            raise _Stop("a message child is still alive; port B cannot be taken")
        try:
            self.collector = _Collector(
                self.o.output_dir / "collector.sqlite", _stub_bind_host(), self.stub_port
            )
        except OSError as exc:
            self.bind_errors += 1
            raise _Stop(f"the collector could not bind port B: {type(exc).__name__}") from exc
        opened = datetime.now(UTC)
        self.windows.append(
            {
                "repetition": repetition,
                "opened_at": _now(),
                "closed_at": "",
                "message_children_alive": self.live_children,
            }
        )
        if not self.synthetic:
            self.synthetic_post()
        return opened

    def close_window(self) -> None:
        if self.collector is not None:
            self.collector.close()
            self.collector = None
        if self.windows and not self.windows[-1]["closed_at"]:
            self.windows[-1]["closed_at"] = _now()

    def observe_deliveries(self, repetition: int) -> None:
        pending = [p for p in self.pending if p["scope"]["repetition"] == repetition]
        assert self.ledger is not None
        self.ledger.observe(self.ledger.provider.usage())
        self.window_usage = self.ledger.last_usage
        window_opened = self.open_window(repetition)
        try:
            self.collect(repetition, pending, window_opened)
        finally:
            self.close_window()
        self.f4_base = None

    def collect(
        self, repetition: int, pending: list[dict[str, Any]], window_opened: datetime
    ) -> None:
        assert self.collector is not None
        if not pending:
            return
        rows: dict[str, dict[str, Any]] = {}
        latest = datetime.now(UTC)
        for item in pending:
            found, _ = self.schedule_row(item["scope"], item["schedule_id"])
            row = found if found is not None else dict(item["row"])
            rows[item["schedule_id"]] = row
            due_at = _utc(row.get("run_at") or item["created"], "run_at")
            latest = max(latest, due_at + timedelta(seconds=DELIVERY_TOLERANCE_SECONDS))
        wait_until = latest + timedelta(seconds=2 * NORMAL_POLL_SECONDS)
        while datetime.now(UTC) < wait_until:
            time.sleep(min(30.0, max(1.0, (wait_until - datetime.now(UTC)).total_seconds())))
        receipts = self.collector.receipts()
        self.close_window()
        window_settled = False
        for item in pending:
            scope = item["scope"]
            found, source = self.schedule_row(scope, item["schedule_id"])
            if source == "error":
                self.failures.append(
                    f"{scope['case_id']} {item['schedule_id']} ERROR: the schedule read failed"
                )
            stored = [found] if found is not None else []
            row = found if found is not None else rows[item["schedule_id"]]
            run_at = str(row.get("run_at") or "")
            due_at = _utc(run_at, "run_at") if run_at.endswith("Z") else datetime.now(UTC)
            start = _utc(item["created"], "created")
            end = datetime.now(UTC)
            one_shot = row.get("cron") is None and bool(run_at)
            due = one_shot and row.get("state") != "cancelled" and start <= due_at <= end
            mine = [
                dict(r)
                for r in receipts
                if not r["synthetic"]
                and r["channel"] == item["channel"]
                and item["token"] in r["text"]
            ]
            evidence = self.add(
                "schedule-observation",
                {
                    **scope,
                    "schedule_id": item["schedule_id"],
                    "task_token": item["token"],
                    "channel": item["channel"],
                    "zone": row.get("timezone"),
                    "kind": "one-shot" if one_shot else "recurring",
                    "due_at": f"{due_at:%Y-%m-%dT%H:%M:%S}Z",
                    "due": due,
                    "state": row.get("state"),
                    "normal_poll_seconds": NORMAL_POLL_SECONDS,
                    "observation_start": item["created"],
                    "observation_end": _now(),
                    "tolerance_seconds": DELIVERY_TOLERANCE_SECONDS,
                    "delivery_route": {
                        "channel": item["channel"],
                        "worker_slack_api_port": self.stub_port,
                    },
                    "collector_receipts": mine,
                    "cli_snapshot": {"agent_schedules": stored, "source": source},
                },
            )
            self.delivery_rows.append(
                {
                    **scope,
                    "schedule_id": item["schedule_id"],
                    "due": due,
                    "delivery_count": len(mine),
                    "receipt_times": [r["received_at"] for r in mine],
                    "delivery_ids": [r["id"] for r in mine],
                    "schedule_evidence": evidence,
                }
            )
            if due and window_opened > due_at:
                self.failures.append(
                    f"{scope['case_id']} {item['schedule_id']} fell due while a message child "
                    "still held port B"
                )
            cancelled = row.get("state") == "cancelled"
            if row.get("state") == "active":
                with contextlib.suppress(_Stop):
                    self.must(
                        [
                            "local",
                            "schedules",
                            "--agent",
                            scope["agent_id"],
                            "--cancel",
                            item["schedule_id"],
                        ],
                        scope,
                    )
                    cancelled = True
            self.settle_schedule(
                item,
                fired=bool(mine) or row.get("state") not in ("active", "cancelled"),
                cancelled=cancelled,
                first=not window_settled,
            )
            window_settled = (
                window_settled or bool(mine) or row.get("state") not in ("active", "cancelled")
            )

    def settle_schedule(
        self, item: Mapping[str, Any], *, fired: bool, cancelled: bool, first: bool
    ) -> None:
        """Release a fire reservation only once the fire provably cannot run; a fired
        schedule is charged with the window's usage movement, and the first fired item
        carries the whole window delta so later fires add nothing twice."""

        assert self.ledger is not None
        if item.get("operation") is None or item["operation"] not in self.open_fires:
            return
        operation = str(item["operation"])
        self.open_fires.discard(operation)
        if not fired:
            if cancelled:
                self.ledger.release(operation, f"schedule {item['schedule_id']} cancelled unfired")
            else:
                self.open_fires.add(operation)
            return
        if first:
            self.ledger.settle(operation, self.window_usage, None, record=operation)
        else:
            self.ledger.settle(operation, self.ledger.last_usage, Decimal(0), record=operation)
            self.ledger.notes.append(
                f"{_now()} {operation} fired in the same window; its cost is inside the "
                "window delta already charged"
            )

    # Whole lifecycle --------------------------------------------------------------

    def candidate_commit(self) -> str:
        completed = subprocess.run(
            ["git", "-C", str(self.root), "rev-parse", "HEAD"],
            capture_output=True,
            timeout=60,
            check=False,
            env=_docker_env(),
            stdin=subprocess.DEVNULL,
        )
        commit = completed.stdout.decode().strip()
        if completed.returncode != 0 or not COMMIT_RE.fullmatch(commit):
            raise _Stop("the candidate commit could not be associated with the build")
        return commit

    def plan(self) -> dict[str, Any]:
        return {
            "dry_run": True,
            "binary": dict(self.binary),
            "source_root": str(self.root),
            "source_tree_sha256": self.source_sha,
            "output_dir": str(self.o.output_dir),
            "repetitions": self.o.repetitions,
            "budget_usd": str(self.o.budget),
            "thresholds": dict(self.o.thresholds),
            "custom_thresholds": _is_custom(self.o.thresholds),
            "models": {"actor": ACTOR_MODEL, "judge": JUDGE_MODEL, "provider_host": PROVIDER_HOST},
            "capabilities": {k: v.get("available", "unknown") for k, v in self.capabilities.items()}
            or "unknown",
            "ownership": {
                "project_prefix": self.ownership.project_prefix,
                "compose_files": [
                    str(self.base_compose),
                    str(self.o.output_dir / "compose-override-rN.yaml"),
                ],
            },
            "command_sequence": [
                "OpenRouter catalog and key preflight",
                "local up --build --model " + ACTOR_MODEL,
                "runner resolver preflight",
                "judge calibration",
                "per scenario: local deploy, "
                "local overrides, local memory --guidance-from, ordinary local message turns, "
                "CLI state reads, controls",
                "real-clock delivery observation",
                "local down --wipe --yes",
                "ownership absence check",
            ],
            "reports": [
                str(self.o.output_dir / "report.json"),
                str(self.o.output_dir / "report.md"),
            ],
            "cost_policy": {
                "campaign_budget_usd": str(self.o.budget),
                "agent_daily_cap_usd": str(AGENT_DAILY_USD),
                "agent_output_tokens_per_run": AGENT_OUTPUT_TOKENS,
                "actor_reserve": "full native per-session cap plus a priced final-generation "
                "overshoot, one turn in flight;"
                " each expected scheduled fire reserves the same",
                "judge_reserve": "priced from the prompt and the 1536-token ceiling",
                "shutdown_reserve_usd": str(SHUTDOWN_RESERVE),
                "judge_max_tokens": JUDGE_MAX_TOKENS,
            },
        }

    def execute(self) -> dict[str, Any]:
        self.o.output_dir.mkdir(parents=True, exist_ok=True)
        commit = ""
        cleanup: dict[str, Any] = {"verified": False}
        previous = {n: signal.getsignal(n) for n in (signal.SIGINT, signal.SIGTERM)}

        def interrupt(number: int, frame: object) -> None:
            raise _Stop(f"interrupted by signal {number}")

        for number in previous:
            signal.signal(number, interrupt)
        try:
            try:
                self.identity()
                commit = self.candidate_commit()
                self.discover()
                self.preflight_provider()
                self.ownership.daemon_id = "\n".join(_docker_lines("info", "--format", "{{.ID}}"))
                self.ownership.unrelated = {
                    "containers": _docker_lines(
                        "container", "ls", "--all", "--no-trunc", "--format", "{{.ID}}"
                    ),
                    "volumes": _docker_lines("volume", "ls", "--format", "{{.Name}}"),
                    "networks": _docker_lines("network", "ls", "--no-trunc", "--format", "{{.ID}}"),
                }
                self.stub_port = _free_port()
                ordered = sorted(self.corpus, key=lambda row: str(row["id"])[:2] == "F4")
                for repetition in range(1, self.o.repetitions + 1):
                    self.up(repetition)
                    if repetition == 1:
                        self.resolver_preflight()
                        self.calibrate()
                    for fixture in ordered:
                        self.scenario(repetition, fixture)
                        assert self.stack is not None
                        self.ownership.observe(self.stack)
                    self.observe_deliveries(repetition)
                    self.down(repetition)
            except (_Stop, _Invalid, OSError, subprocess.SubprocessError) as exc:
                self.failures.append(f"stopped: {self.scrub(str(exc))}")
            finally:
                for number, handler in previous.items():
                    signal.signal(number, signal.SIG_IGN if handler is None else handler)
                cleanup = self.teardown()
        finally:
            for number, handler in previous.items():
                signal.signal(number, signal.SIG_DFL if handler is None else handler)
        return self.report(commit, cleanup)

    def synthetic_post(self) -> None:
        assert self.collector is not None
        request = urllib.request.Request(
            f"http://127.0.0.1:{self.collector.port}/api/chat.postMessage",
            data=json.dumps(
                {"channel": SYNTHETIC_CHANNEL, "text": "synthetic transport check"}
            ).encode(),
            headers={"Content-Type": "application/json", "X-Learning-Synthetic": "1"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30) as response:  # noqa: S310
            body = _obj(json.loads(response.read()), "collector reply")
        receipt = next(r for r in self.collector.receipts() if r["id"] == body.get("ts"))
        self.synthetic = {
            "synthetic": True,
            "received_at": receipt["received_at"],
            "label": "synthetic collector transport proof",
        }

    def teardown(self) -> dict[str, Any]:
        problems: list[str] = []
        if self.stack is not None:
            # `local down --wipe` runs for the same project, compose files and
            # platform key that `local up` used for this stack.
            repetition = self.stack.repetition
            try:
                self.down(repetition)
            except (_Stop, _Invalid, OSError, subprocess.SubprocessError) as exc:
                problems.append(f"local down: {self.scrub(str(exc))}")
        self.close_window()
        try:
            problems += self.ownership.remove_leftovers()
        except _Invalid as exc:
            problems.append(str(exc))
        cleanup = {
            "verified": False,
            "ownership_manifest": self.ownership.manifest(),
            "unrelated_before": self.ownership.unrelated,
            "problems": problems,
        }
        try:
            _check_cleanup_live(_Report(path=self.o.output_dir, data={"cleanup": cleanup}))
            cleanup["verified"] = not problems
        except _Invalid as exc:
            problems.append(str(exc))
        if not cleanup["verified"]:
            self.failures.append("cleanup could not be verified: " + "; ".join(problems))
        return cleanup

    def report(self, commit: str, cleanup: dict[str, Any]) -> dict[str, Any]:
        with contextlib.suppress(_Stop, OSError):
            if self.binary:
                self.recheck_identity()
        accounting: dict[str, Any] = {"budget_usd": str(self.o.budget), "operations": []}
        if self.ledger is not None:
            try:
                accounting = self.ledger.final()
            except _Stop as exc:
                self.failures.append(f"final accounting: {exc}")
        thresholds = self.o.thresholds
        repetitions = [
            {
                "repetition": rep,
                **_summary([d for d in self.decisions if d["repetition"] == rep], thresholds),
            }
            for rep in range(1, self.o.repetitions + 1)
        ]
        metrics = {**_summary(self.decisions, thresholds), "repetitions": repetitions}
        target = int(thresholds["scheduled_deliveries"])
        due = [row for row in self.delivery_rows if row["due"]]
        once = sum(row["delivery_count"] == target for row in due)
        caps_complete = bool(self.capabilities) and all(
            v.get("available") for v in self.capabilities.values()
        )
        verification = "PASS" if not self.failures else "FAIL"
        acceptance = "NOT_AVAILABLE" if not caps_complete else verification
        for record in self.evidence.values():
            record["sha256"] = _sha(_json_bytes(record["value"]))
        report: dict[str, Any] = {
            "report_version": REPORT_VERSION,
            "repetitions": self.o.repetitions,
            "seed": self.run_id,
            "data_origin": "live-campaign",
            "implementation_verification": verification,
            "acceptance": acceptance,
            "exit_code": 0 if verification == "PASS" else 1,
            "failure_reasons": [self.scrub(f) for f in self.failures],
            "binary": self.binary,
            "source_root": str(self.root),
            "source_tree_sha256": self.source_sha,
            "source_manifest": self.source_manifest,
            "build_association": {
                "candidate_commit": commit,
                "input_manifest_sha256": self.source_sha,
                "binary_sha256": self.binary.get("sha256", ""),
            },
            "fixtures": {
                name: {"path": str(FIXTURES / name), "sha256": _file_sha(FIXTURES / name)}
                for name in ("learning-cases.json", "learning-calibration.json")
            },
            "bundle_sha256": self.bundle_sha,
            "guidance_sha256": self.guidance_sha,
            "applied_guidance_sha256": _sha(_applied_guidance(str(self.guidance["text"])).encode()),
            "output_dir": str(self.o.output_dir),
            "thresholds": dict(thresholds),
            "custom_thresholds": _is_custom(thresholds),
            "models": {
                "actor": ACTOR_MODEL,
                "judge": JUDGE_MODEL,
                "resolved_hostname": self.resolved.get("host"),
                "resolved_route": self.resolved,
            },
            "capabilities": self.capabilities,
            "memory_schema": self.memory_schema,
            "evidence": self.evidence,
            "invocations": self.invocations,
            "cases": self.cases,
            "decisions": self.decisions,
            "procedure_probes": {"version": 1, "rows": self.procedure_rows},
            "memory_negative": {"version": 1, "rows": self.negative_rows},
            "deliveries": {"version": 1, "rows": self.delivery_rows},
            "delivery_metrics": {
                "due_schedules": len(due),
                "delivered_exactly_once": once,
                "delivery_rate": once / len(due) if due else None,
            },
            "metrics": metrics,
            "controls": self.controls,
            "generations": self.generations,
            "calibration": self.calibration,
            "accounting": accounting,
            "cleanup": cleanup,
            "collector": {
                "port_b": self.stub_port,
                "bind_errors": self.bind_errors,
                "recovered_bind_retries": self.recovered_bind_retries,
                "delivery_windows": [w for w in self.windows if w["closed_at"]],
                "synthetic_transport_receipt": self.synthetic,
            },
            "runner_turn_bound": "native runner default, not overridden by this harness",
            "setup_retries": self.setup_retries,
            "agent_caps": self.agent_caps,
            "worker": self.worker,
            "schedule_reservations": self.schedule_reservations,
            "quiesce_actions": self.quiesce_actions,
        }
        path = self.o.output_dir / "report.json"
        path.write_bytes(_json_bytes(report))
        if verification == "PASS":
            check = verify_report(path)
            if not check.passed:
                report["implementation_verification"] = "FAIL"
                report["acceptance"] = "NOT_AVAILABLE" if not caps_complete else "FAIL"
                report["exit_code"] = 1
                report["failure_reasons"].append(f"self-check: {check.detail}")
                path.write_bytes(_json_bytes(report))
        (self.o.output_dir / "report.md").write_text(_markdown(report), encoding="utf-8")
        return report


def _markdown(report: Mapping[str, Any]) -> str:
    metrics = report["metrics"]
    accounting = report["accounting"]
    lines = [
        "# Learning evaluation report",
        "",
        f"Implementation verification: {report['implementation_verification']}",
        f"Acceptance: {report['acceptance']}",
        f"Repetitions: {report['repetitions']}",
        f"Actor: {ACTOR_MODEL}. Judge: {JUDGE_MODEL}.",
        "",
        "## Capabilities",
        "",
    ]
    lines += [
        f"{kind}: {'available' if entry.get('available') else 'not available'}"
        for kind, entry in report["capabilities"].items()
    ]
    lines += ["", "## Gates", ""]
    lines += [
        f"{name}: {gate['status']} ({gate['value']})" for name, gate in metrics["gates"].items()
    ]
    delivery = report["delivery_metrics"]
    lines += [
        f"scheduled delivery rate: {delivery['delivery_rate']} "
        f"({delivery['delivered_exactly_once']} of {delivery['due_schedules']})",
        "",
    ]
    lines += [
        "## Cost",
        "",
        f"Budget {accounting.get('budget_usd')} USD, charged "
        f"{accounting.get('charged_usd', 'unknown')} USD "
        f"({accounting.get('basis', 'unknown basis')}).",
        "",
    ]
    per_case: dict[str, Decimal] = {}
    for generation in report["generations"]:
        case = str(generation.get("case_id") or "calibration")
        per_case[case] = per_case.get(case, Decimal(0)) + Decimal(str(generation["cost_usd"]))
    lines += [f"{case}: {amount} USD" for case, amount in sorted(per_case.items())]
    lines += ["", "## Failures", ""]
    lines += [str(reason) for reason in report["failure_reasons"]] or ["none"]
    lines += ["", f"Cleanup verified: {report['cleanup'].get('verified')}", ""]
    return "\n".join(lines)


# Command line ------------------------------------------------------------------------


class _Usage(ValueError):
    """A rejected argument; reported on stderr with no stdout."""


def _number(text: str, label: str) -> int | float:
    try:
        value: int | float = int(text)
    except ValueError:
        try:
            value = float(text)
        except ValueError as exc:
            raise _Usage(f"{label} must be a number") from exc
    if isinstance(value, float) and not math.isfinite(value):
        raise _Usage(f"{label} must be finite")
    return value


def _pinned_binary(text: str) -> Path:
    path = Path(text)
    if not path.is_absolute():
        raise _Usage("--curie-bin must be an absolute path to the candidate executable")
    if not path.is_file() or not os.access(path, os.X_OK):
        raise _Usage("--curie-bin is not an executable file")
    return path


def _run_options(args: argparse.Namespace) -> _Options:
    curie_bin = _pinned_binary(args.curie_bin)
    if args.repetitions < 1:
        raise _Usage("--repetitions must be at least 1")
    try:
        budget = Decimal(args.budget_usd)
    except InvalidOperation as exc:
        raise _Usage("--budget-usd must be a decimal amount") from exc
    if not budget.is_finite() or not Decimal(0) < budget <= MAX_BUDGET:
        raise _Usage("--budget-usd must be within (0, 5.00]")
    thresholds: dict[str, Any] = {}
    for name, flag in (
        ("precision", "precision_threshold"),
        ("recall", "recall_threshold"),
        ("falsewrite", "falsewrite_threshold"),
        ("heldout_control_improvement", "heldout_control_improvement_threshold"),
    ):
        value = _number(getattr(args, flag), f"--{flag.replace('_', '-')}")
        if not 0 <= value <= 1:
            raise _Usage(f"--{flag.replace('_', '-')} must be within [0, 1]")
        thresholds[name] = value
    duplicates = _number(args.duplicate_threshold, "--duplicate-threshold")
    deliveries = _number(args.scheduled_delivery_threshold, "--scheduled-delivery-threshold")
    if not isinstance(duplicates, int) or duplicates < 0:
        raise _Usage("--duplicate-threshold must be a nonnegative integer")
    if not isinstance(deliveries, int) or deliveries < 1:
        raise _Usage("--scheduled-delivery-threshold must be a positive integer")
    thresholds["correction_duplicates"] = duplicates
    thresholds["scheduled_deliveries"] = deliveries
    output = Path(args.output_dir)
    if not output.is_absolute():
        raise _Usage("--output-dir must be an absolute path")
    order = list(DEFAULT_THRESHOLDS)
    return _Options(
        curie_bin,
        args.repetitions,
        budget,
        output,
        {k: thresholds[k] for k in order},
        args.dry_run,
        args.json,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="learning-eval", allow_abbrev=False)
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", allow_abbrev=False)
    run.add_argument("--curie-bin", required=True)
    run.add_argument("--repetitions", type=int, default=1)
    run.add_argument("--budget-usd", default="5.00")
    run.add_argument("--precision-threshold", default="0.9")
    run.add_argument("--recall-threshold", default="0.8")
    run.add_argument("--falsewrite-threshold", default="0.05")
    run.add_argument("--duplicate-threshold", default="0")
    run.add_argument("--heldout-control-improvement-threshold", default="0")
    run.add_argument("--scheduled-delivery-threshold", default="1")
    run.add_argument("--output-dir", required=True)
    run.add_argument("--dry-run", action="store_true")
    run.add_argument("--json", action="store_true")
    for name in ("report", "calibration", "negative", "cleanup"):
        validator = commands.add_parser(f"verify-{name}", allow_abbrev=False)
        validator.add_argument("report")
        validator.add_argument("--curie-bin", required=True)
        validator.add_argument("--json", action="store_true")
    return parser


_VALIDATORS: dict[str, Callable[[Path], ScoreResult]] = {
    "verify-report": verify_report,
    "verify-calibration": verify_calibration,
    "verify-negative": verify_negative,
    "verify-cleanup": verify_cleanup,
}


def _validate_command(args: argparse.Namespace) -> int:
    curie_bin = _pinned_binary(args.curie_bin)
    path = Path(args.report)
    result = _VALIDATORS[args.command](path)
    if result.passed and args.command == "verify-report":
        # The public function accepts labeled synthetic vectors for unit tests;
        # the operator validator only accepts a live campaign's report.
        try:
            origin = _obj(_read_json(path), "report").get("data_origin")
        except (_Invalid, OSError, ValueError):
            origin = None
        if origin != "live-campaign":
            result = ScoreResult(passed=False, detail="the report is not from a live campaign")
    if result.passed:
        try:
            recorded = _obj(_obj(_read_json(path), "report").get("binary"), "binary")
            same = recorded.get("path") == str(curie_bin) and recorded.get("sha256") == _file_sha(
                curie_bin
            )
        except (_Invalid, OSError, ValueError):
            same = False
        if not same:
            result = ScoreResult(
                passed=False, detail="the dispatching executable is not the recorded one"
            )
    if not result.passed:
        print(f"{args.command}: {result.detail}", file=sys.stderr)
        return 1
    if args.json:
        print(json.dumps({"passed": True, "detail": result.detail}))
    else:
        print(f"{args.command}: passed")
    return 0


def _run_command(args: argparse.Namespace) -> int:
    options = _run_options(args)
    key = os.environ.get("CURIE_CREDENTIALS") or None
    for name in STRIPPED_AUTH:
        os.environ.pop(name, None)
    campaign = _Campaign(options, key)
    if options.dry_run:
        campaign.identity()
        with contextlib.suppress(_Stop, _Invalid, ValueError, OSError):
            campaign.discover()
        plan = campaign.plan()
        print(
            json.dumps(plan)
            if options.json
            else "\n".join(f"{k}: {json.dumps(v)}" for k, v in plan.items())
        )
        return 0
    report = campaign.execute()
    location = options.output_dir / "report.json"
    if report["implementation_verification"] != "PASS":
        reasons = "; ".join(report["failure_reasons"][:5])
        print(_bounded(f"learning eval did not pass ({reasons}); see {location}"), file=sys.stderr)
        return 1
    if options.json:
        print(json.dumps(report))
    else:
        print(f"learning eval passed, acceptance {report['acceptance']}; see {location}")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Entry for ``python -m curie_worker.eval.learning``."""

    parser = _parser()
    try:
        args = parser.parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else 2
    try:
        if args.command == "run":
            return _run_command(args)
        return _validate_command(args)
    except _Usage as exc:
        print(f"learning-eval: {exc}", file=sys.stderr)
        return 2
    except (_Stop, _Invalid, OSError, subprocess.SubprocessError) as exc:
        print(f"learning-eval: {_bounded(str(exc))}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
