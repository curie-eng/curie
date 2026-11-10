"""The mean tester's ship gate is a deterministic script, not the model's judgement.

`examples/mean-tester/gate/mean_tester_gate.py` (installed in the runner layer as
`mean-tester-gate`) validates the target's fixed acceptance suite, keeps the
ledger of per-probe verdicts the tester records, and alone decides GO or NO-GO.
The model judges each probe and copies the gate's `Ship:` and `Ledger:` lines.

Every test drives the script as a subprocess, the way the tester does:
exit 0 is GO / READY / success, 1 is NO-GO / not READY, 2 is a usage error or a
refused input. `scenario`, `record`, `verdict` and `import` bind the ledger to
one campaign id, so a stale ledger from an earlier campaign is refused.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

import jsonschema
import pytest

REPO = Path(__file__).resolve().parents[2]
BUNDLE = REPO / "examples" / "mean-tester"
GATE = BUNDLE / "gate" / "mean_tester_gate.py"
SHIPPED_SUITE = BUNDLE / "acceptance" / "cases.json"
SCHEMA = json.loads((BUNDLE / "acceptance" / "schema.json").read_text())
CAMPAIGN = "a1b2"


# --------------------------------------------------------------------------- helpers


def _run(*args: str, timeout: float = 60) -> subprocess.CompletedProcess:
    assert GATE.is_file(), f"the gate script is missing: {GATE}"
    return subprocess.run(
        [sys.executable, str(GATE), *args],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _case(case_id: str, criterion: str, priority: str = "P0", repeat: int = 1) -> dict:
    return {
        "id": case_id,
        "probe": f"Probe for {case_id}: what can you tell me?",
        "mode": "read-or-ask",
        "attachments": [],
        "expected_reply": [f"Answers {case_id} with read evidence."],
        "card_action": None,
        "expected_state": None,
        "criterion": criterion,
        "priority": priority,
        "repeat": repeat,
    }


def _base_suite() -> dict:
    """A valid, read-only suite: c1 is P0 with two repeats, c2 is P1 with one."""

    return {
        "version": 1,
        "name": "gate-test-suite",
        "criteria": [
            {"id": "answers", "description": "It answers ordinary questions."},
            {"id": "refuses", "description": "It refuses what it must not do."},
        ],
        "cases": [
            _case("c1", "answers", "P0", 2),
            _case("c2", "refuses", "P1", 1),
        ],
    }


def _write(path: Path, suite) -> Path:
    path.write_text(suite if isinstance(suite, str) else json.dumps(suite, indent=2))
    return path


def _suite(tmp_path: Path, suite=None, name: str = "cases.json") -> Path:
    return _write(tmp_path / name, _base_suite() if suite is None else suite)


def _intake(suite: Path, *extra: str) -> tuple[subprocess.CompletedProcess, dict]:
    result = _run("intake", "--suite", str(suite), *extra)
    return result, json.loads(result.stdout)


def _blob_sha(path: Path) -> str:
    data = path.read_bytes()
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def _ok(result: subprocess.CompletedProcess) -> subprocess.CompletedProcess:
    assert result.returncode == 0, (result.returncode, result.stdout, result.stderr)
    return result


def _bound(command: str, suite: Path, ledger: Path, *args: str,
           campaign: str | None = CAMPAIGN) -> subprocess.CompletedProcess:
    """A ledger command, bound to the campaign unless campaign is None."""

    binding = () if campaign is None else ("--campaign", campaign)
    return _run(command, "--suite", str(suite), "--ledger", str(ledger), *binding, *args)


def _record_case(suite: Path, ledger: Path, case: str, repeat: int, verdict: str,
                 *extra: str, campaign: str | None = CAMPAIGN):
    return _bound("record", suite, ledger, "--case", case, "--repeat", str(repeat),
                  "--verdict", verdict, *extra, campaign=campaign)


def _record_step(suite: Path, ledger: Path, scenario: str, step: int, verdict: str,
                 *extra: str, campaign: str | None = CAMPAIGN):
    return _bound("record", suite, ledger, "--scenario", scenario, "--step", str(step),
                  "--verdict", verdict, *extra, campaign=campaign)


def _record_probe(suite: Path, ledger: Path, label: str, verdict: str,
                  *extra: str, campaign: str | None = CAMPAIGN):
    return _bound("record", suite, ledger, "--probe", label, "--verdict", verdict,
                  *extra, campaign=campaign)


def _plan(suite: Path, ledger: Path, *labels: str, extra: tuple = (),
          campaign: str | None = CAMPAIGN):
    probes = [arg for label in labels for arg in ("--probe", label)]
    return _bound("plan", suite, ledger, *probes, *extra, campaign=campaign)


def _declare(suite: Path, ledger: Path, name: str, steps: int,
             *extra: str, campaign: str | None = CAMPAIGN):
    return _bound("scenario", suite, ledger, "--name", name, "--steps", str(steps),
                  *extra, campaign=campaign)


def _verdict(suite: Path, ledger: Path, *extra: str,
             campaign: str | None = CAMPAIGN) -> subprocess.CompletedProcess:
    return _bound("verdict", suite, ledger, *extra, campaign=campaign)


def _import(suite: Path, ledger: Path, token: str, *extra: str,
            campaign: str | None = CAMPAIGN) -> subprocess.CompletedProcess:
    return _bound("import", suite, ledger, "--token", token, *extra, campaign=campaign)


def _coverage(result: subprocess.CompletedProcess) -> str:
    found = re.search(r"^Coverage:.*$", result.stdout, re.M)
    assert found, result.stdout
    return found.group(0)


def _ship(result: subprocess.CompletedProcess) -> str:
    return result.stdout.splitlines()[0] if result.stdout else ""


def _token(result: subprocess.CompletedProcess) -> str:
    found = re.search(r"^Ledger: (mt1\.\S+)", result.stdout, re.M)
    assert found, result.stdout
    return found.group(1)


# The default all-PASS campaign over the base suite.
CASE_RUNS = [("c1", 1), ("c1", 2), ("c2", 1)]
SCENARIOS = {"onboarding": 2, "month-end": 1}


def _campaign(
    suite: Path,
    ledger: Path,
    *,
    case_verdicts: dict | None = None,
    skip_cases: tuple = (),
    scenarios: dict | None = None,
    step_verdicts: dict | None = None,
    skip_steps: tuple = (),
    probes: dict | None = None,
) -> None:
    """Record a campaign; everything PASS unless overridden or skipped."""

    case_verdicts = case_verdicts or {}
    for label, verdict in (probes or {}).items():
        _ok(_record_probe(suite, ledger, label, verdict))
    step_verdicts = step_verdicts or {}
    for case, repeat in CASE_RUNS:
        if (case, repeat) in skip_cases:
            continue
        verdict = case_verdicts.get((case, repeat), "PASS")
        _ok(_record_case(suite, ledger, case, repeat, verdict))
    for name, steps in (SCENARIOS if scenarios is None else scenarios).items():
        _ok(_declare(suite, ledger, name, steps))
        for step in range(1, steps + 1):
            if (name, step) in skip_steps:
                continue
            verdict = step_verdicts.get((name, step), "PASS")
            _ok(_record_step(suite, ledger, name, step, verdict))


# --------------------------------------------------------------------------- intake


def test_a_valid_read_only_suite_is_ready_with_a_plan_in_file_order(tmp_path):
    result, report = _intake(_suite(tmp_path))
    assert result.returncode == 0, result.stdout
    assert report["status"] == "READY"
    assert report["errors"] == []
    assert report["scope"] == "read-only"
    assert isinstance(report["suite_digest"], str) and report["suite_digest"]
    assert report["cases"] == [
        {"id": "c1", "criterion": "answers", "priority": "P0", "repeat": 2,
         "eligible": True, "blocked": None},
        {"id": "c2", "criterion": "refuses", "priority": "P1", "repeat": 1,
         "eligible": True, "blocked": None},
    ]
    assert report["plan"] == [
        {"case": "c1", "repeat": 1},
        {"case": "c1", "repeat": 2},
        {"case": "c2", "repeat": 1},
    ]


def test_a_missing_suite_is_missing(tmp_path):
    result, report = _intake(tmp_path / "nope.json")
    assert result.returncode == 1
    assert report["status"] == "MISSING"
    assert report["scope"] is None
    assert report["errors"]


def _set(path: tuple, value):
    def mutate(suite):
        target = suite
        for key in path[:-1]:
            target = target[key]
        target[path[-1]] = value
        return suite

    return mutate


def _drop(path: tuple):
    def mutate(suite):
        target = suite
        for key in path[:-1]:
            target = target[key]
        del target[path[-1]]
        return suite

    return mutate


def _dup_criterion(suite):
    suite["criteria"].append(copy.deepcopy(suite["criteria"][0]))
    return suite


def _dup_case(suite):
    suite["cases"].append(copy.deepcopy(suite["cases"][0]))
    return suite


# (id, mutation, pure schema violation that schema.json alone must also reject)
MALFORMED = [
    ("invalid-json", lambda s: "{not json", False),
    ("top-level-array", lambda s: [s], True),
    ("version-2", _set(("version",), 2), True),
    ("version-string", _set(("version",), "1"), True),
    ("unknown-top-level-field", _set(("owner",), "someone"), True),
    ("unknown-case-field", _set(("cases", 0, "notes"), "extra"), True),
    ("unknown-criterion-field", _set(("criteria", 0, "weight"), 2), True),
    ("missing-name", _drop(("name",)), True),
    ("missing-criteria", _drop(("criteria",)), True),
    ("missing-cases", _drop(("cases",)), True),
    ("missing-version", _drop(("version",)), True),
    ("missing-criterion-description", _drop(("criteria", 0, "description")), True),
    ("missing-case-probe", _drop(("cases", 0, "probe")), True),
    ("missing-case-repeat", _drop(("cases", 0, "repeat")), True),
    ("missing-case-card-action", _drop(("cases", 0, "card_action")), True),
    ("missing-case-expected-state", _drop(("cases", 0, "expected_state")), True),
    ("missing-case-attachments", _drop(("cases", 0, "attachments")), True),
    ("empty-criteria", _set(("criteria",), []), True),
    ("empty-cases", _set(("cases",), []), True),
    ("empty-name", _set(("name",), ""), True),
    ("empty-probe", _set(("cases", 0, "probe"), ""), True),
    ("empty-expected-reply", _set(("cases", 0, "expected_reply"), []), True),
    ("duplicate-criterion-id", _dup_criterion, False),
    ("duplicate-case-id", _dup_case, False),
    ("unknown-criterion-reference", _set(("cases", 0, "criterion"), "nowhere"), False),
    ("mode-unsupported", _set(("cases", 0, "mode"), "write"), True),
    ("priority-unsupported", _set(("cases", 0, "priority"), "P2"), True),
    ("repeat-bool", _set(("cases", 0, "repeat"), True), True),
    ("repeat-zero", _set(("cases", 0, "repeat"), 0), True),
    ("repeat-negative", _set(("cases", 0, "repeat"), -1), True),
    ("repeat-float", _set(("cases", 0, "repeat"), 1.5), True),
    ("repeat-string", _set(("cases", 0, "repeat"), "2"), True),
    ("card-action-unsupported", _set(("cases", 0, "card_action"), "approve-all"), True),
    ("expected-state-list", _set(("cases", 0, "expected_state"), ["x"]), True),
    ("expected-state-string", _set(("cases", 0, "expected_state"), "done"), True),
    # Unhashable values must be reported, not crash the membership checks.
    ("mode-list", _set(("cases", 0, "mode"), ["action"]), True),
    ("priority-object", _set(("cases", 0, "priority"), {}), True),
    ("criterion-list", _set(("cases", 0, "criterion"), ["answers"]), True),
    # A repeat is capped so a huge one cannot hang intake with its plan.
    ("repeat-101", _set(("cases", 0, "repeat"), 101), True),
    ("repeat-1e15", lambda s: json.dumps(s).replace('"repeat": 2', '"repeat": 1e15'), True),
]


def _mutated(mutation):
    return mutation(_base_suite())


def _as_json(document):
    return json.loads(document) if isinstance(document, str) else document


@pytest.mark.parametrize(
    "mutation", [m for _, m, _ in MALFORMED], ids=[i for i, _, _ in MALFORMED]
)
def test_every_schema_rule_violation_is_malformed(tmp_path, mutation):
    result = _run("intake", "--suite", str(_suite(tmp_path, _mutated(mutation))), timeout=10)
    assert "Traceback" not in result.stderr, result.stderr
    assert result.returncode == 1, result.stdout
    report = json.loads(result.stdout)
    assert report["status"] == "MALFORMED"
    assert report["errors"], report
    assert report["scope"] is None
    assert report["plan"] == []


SCHEMA_ONLY = [(i, m) for i, m, pure in MALFORMED if pure]


def test_the_base_suite_satisfies_schema_json():
    jsonschema.Draft202012Validator(SCHEMA).validate(_base_suite())


@pytest.mark.parametrize(
    "mutation", [m for _, m in SCHEMA_ONLY], ids=[i for i, _ in SCHEMA_ONLY]
)
def test_schema_json_rejects_what_the_gate_rejects(mutation):
    # The gate and acceptance/schema.json describe one format: every pure
    # schema violation the gate refuses, the schema refuses too.
    document = _as_json(_mutated(mutation))
    assert not jsonschema.Draft202012Validator(SCHEMA).is_valid(document)


def test_a_matching_blob_sha_keeps_the_suite_ready(tmp_path):
    suite = _suite(tmp_path)
    result, report = _intake(suite, "--blob-sha", _blob_sha(suite))
    assert result.returncode == 0, result.stdout
    assert report["status"] == "READY"


def test_a_blob_sha_mismatch_is_malformed(tmp_path):
    suite = _suite(tmp_path)
    result, report = _intake(suite, "--blob-sha", "0" * 40)
    assert result.returncode == 1
    assert report["status"] == "MALFORMED"
    assert any("blob" in error.lower() for error in report["errors"]), report["errors"]


def test_the_shipped_illustrative_suite_is_action_scope_with_one_eligible_case():
    result, report = _intake(SHIPPED_SUITE)
    assert result.returncode == 0, result.stdout
    assert report["status"] == "READY"
    assert report["scope"] == "action"
    eligible = {c["id"]: (c["eligible"], c["blocked"]) for c in report["cases"]}
    assert eligible == {"discover-assets": (True, None), "file-report": (False, "slice 2")}
    assert report["plan"] == [
        {"case": "discover-assets", "repeat": 1},
        {"case": "discover-assets", "repeat": 2},
    ]


ACTION_MARKERS = [
    ("mode-action", {"mode": "action"}),
    ("attachments", {"attachments": ["fixtures/report.txt"]}),
    ("card-action", {"card_action": "approve"}),
    ("expected-state", {"expected_state": {"campaigns/report.txt": "present"}}),
]


@pytest.mark.parametrize("fields", [f for _, f in ACTION_MARKERS],
                         ids=[i for i, _ in ACTION_MARKERS])
def test_any_action_marker_blocks_the_case_and_makes_the_scope_action(tmp_path, fields):
    suite = _base_suite()
    suite["cases"][1].update(fields)
    result, report = _intake(_suite(tmp_path, suite))
    assert result.returncode == 0, result.stdout
    assert report["scope"] == "action"
    c2 = next(c for c in report["cases"] if c["id"] == "c2")
    assert (c2["eligible"], c2["blocked"]) == (False, "slice 2")
    assert report["plan"] == [{"case": "c1", "repeat": 1}, {"case": "c1", "repeat": 2}]


def test_a_flagged_action_case_flips_a_read_only_suite_to_action(tmp_path):
    result, report = _intake(_suite(tmp_path), "--action-case", "c2")
    assert result.returncode == 0, result.stdout
    assert report["scope"] == "action"
    c2 = next(c for c in report["cases"] if c["id"] == "c2")
    assert (c2["eligible"], c2["blocked"]) == (False, "slice 2")
    assert {"case": "c2", "repeat": 1} not in report["plan"]


def test_flagging_an_unknown_case_is_refused(tmp_path):
    result = _run("intake", "--suite", str(_suite(tmp_path)), "--action-case", "nope")
    assert result.returncode == 2


# --------------------------------------------------------------------------- verdict


def test_everything_passing_is_go_read_only_scope(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    result = _verdict(suite, ledger)
    assert result.returncode == 0, result.stdout
    assert _ship(result).startswith("Ship: GO (read-only scope)"), result.stdout
    assert _token(result).startswith("mt1.")


def _no_go(result: subprocess.CompletedProcess, reason: str) -> None:
    assert result.returncode == 1, result.stdout
    ship = _ship(result)
    assert ship.startswith("Ship: NO-GO"), result.stdout
    assert reason in ship, ship


def test_a_missing_ledger_is_empty_so_nothing_has_run(tmp_path):
    suite = _suite(tmp_path)
    result = _verdict(suite, tmp_path / "absent.json")
    _no_go(result, "NOT RUN")
    assert _token(result).startswith("mt1.")


def test_an_unrecorded_repeat_is_not_run(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, skip_cases=(("c1", 2),))
    _no_go(_verdict(suite, ledger), "NOT RUN")


def test_a_p0_repeat_that_fails_after_a_pass_is_no_go(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, case_verdicts={("c1", 2): "FAIL"})
    _no_go(_verdict(suite, ledger), "FAIL")


def test_an_unclear_case_is_no_go(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, case_verdicts={("c2", 1): "UNCLEAR"})
    _no_go(_verdict(suite, ledger), "UNCLEAR")


def test_one_scenario_is_too_few(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, scenarios={"onboarding": 2})
    _no_go(_verdict(suite, ledger), "scenario")


def test_five_scenarios_are_too_many(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, scenarios={f"s{n}": 1 for n in range(1, 6)})
    _no_go(_verdict(suite, ledger), "scenario")


def test_a_blocked_scenario_step_is_no_go(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, step_verdicts={("onboarding", 2): "BLOCKED"})
    _no_go(_verdict(suite, ledger), "BLOCKED")


def test_an_unrecorded_scenario_step_is_not_run(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, skip_steps=(("onboarding", 2),))
    _no_go(_verdict(suite, ledger), "NOT RUN")


def test_an_explicit_gap_is_no_go_and_named(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    gap = "criterion refuses has no spec line"
    _no_go(_verdict(suite, ledger, "--gap", gap), gap)


def test_an_action_scope_suite_is_no_go_until_slice_2(tmp_path):
    ledger = tmp_path / "ledger.json"
    for repeat in (1, 2):
        _ok(_record_case(SHIPPED_SUITE, ledger, "discover-assets", repeat, "PASS"))
    for name, steps in SCENARIOS.items():
        _ok(_declare(SHIPPED_SUITE, ledger, name, steps))
        for step in range(1, steps + 1):
            _ok(_record_step(SHIPPED_SUITE, ledger, name, step, "PASS"))
    _no_go(_verdict(SHIPPED_SUITE, ledger), "slice 2")


def test_a_flagged_action_case_turns_go_into_slice_2_no_go(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    _no_go(_verdict(suite, ledger, "--action-case", "c2"), "slice 2")


def test_a_missing_suite_is_no_go(tmp_path):
    _no_go(_verdict(tmp_path / "nope.json", tmp_path / "ledger.json"), "MISSING")


def test_a_malformed_suite_is_no_go(tmp_path):
    suite = _suite(tmp_path, _set(("version",), 2)(_base_suite()))
    _no_go(_verdict(suite, tmp_path / "ledger.json"), "MALFORMED")


def test_a_blob_sha_mismatch_at_verdict_is_no_go(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    _no_go(_verdict(suite, ledger, "--blob-sha", "0" * 40), "MALFORMED")


# --------------------------------------------------------------------------- record refusals


def _seeded(tmp_path: Path) -> tuple[Path, Path]:
    """A base suite and a ledger with c1 repeat 1, onboarding step 1 and the
    probe `seed.1` recorded."""

    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _ok(_record_case(suite, ledger, "c1", 1, "PASS"))
    _ok(_declare(suite, ledger, "onboarding", 2))
    _ok(_record_step(suite, ledger, "onboarding", 1, "PASS"))
    _ok(_record_probe(suite, ledger, "seed.1", "PASS"))
    return suite, ledger


REFUSALS = [
    ("unknown-case", ("--case", "nope", "--repeat", "1", "--verdict", "PASS")),
    ("repeat-above-declared", ("--case", "c1", "--repeat", "3", "--verdict", "PASS")),
    ("repeat-zero", ("--case", "c1", "--repeat", "0", "--verdict", "PASS")),
    ("duplicate-case-repeat", ("--case", "c1", "--repeat", "1", "--verdict", "PASS")),
    ("duplicate-case-repeat-new-verdict",
     ("--case", "c1", "--repeat", "1", "--verdict", "FAIL")),
    ("blocked-verdict-for-a-case", ("--case", "c2", "--repeat", "1", "--verdict", "BLOCKED")),
    ("undeclared-scenario", ("--scenario", "nope", "--step", "1", "--verdict", "PASS")),
    ("step-above-declared", ("--scenario", "onboarding", "--step", "3", "--verdict", "PASS")),
    ("step-zero", ("--scenario", "onboarding", "--step", "0", "--verdict", "PASS")),
    ("duplicate-step", ("--scenario", "onboarding", "--step", "1", "--verdict", "PASS")),
    ("case-with-a-step", ("--case", "c1", "--step", "1", "--verdict", "PASS")),
    ("scenario-with-a-repeat",
     ("--scenario", "onboarding", "--repeat", "1", "--verdict", "PASS")),
    ("blocked-verdict-for-a-probe", ("--probe", "edge.1", "--verdict", "BLOCKED")),
    ("duplicate-probe", ("--probe", "seed.1", "--verdict", "FAIL")),
    ("probe-label-leading-underscore", ("--probe", "_edge", "--verdict", "PASS")),
    ("probe-label-with-a-space", ("--probe", "edge 1", "--verdict", "PASS")),
    ("probe-label-with-a-slash", ("--probe", "edge/1", "--verdict", "PASS")),
    ("unknown-action-case", ("--case", "c2", "--repeat", "1", "--verdict", "PASS",
                             "--action-case", "nope")),
]


@pytest.mark.parametrize("args", [a for _, a in REFUSALS], ids=[i for i, _ in REFUSALS])
def test_a_refused_record_leaves_the_ledger_byte_identical(tmp_path, args):
    suite, ledger = _seeded(tmp_path)
    before = ledger.read_bytes()
    result = _bound("record", suite, ledger, *args)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert ledger.read_bytes() == before


def test_recording_a_blocked_action_case_is_refused(tmp_path):
    suite = _base_suite()
    suite["cases"].append({**_case("act", "refuses"), "mode": "action"})
    path, ledger = _suite(tmp_path, suite), tmp_path / "ledger.json"
    _ok(_declare(path, ledger, "onboarding", 1))
    before = ledger.read_bytes()
    result = _record_case(path, ledger, "act", 1, "PASS")
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert ledger.read_bytes() == before


def test_recording_against_a_suite_that_is_not_ready_is_refused(tmp_path):
    _, ledger = _seeded(tmp_path)
    broken = _suite(tmp_path, _set(("version",), 2)(_base_suite()), name="broken.json")
    before = ledger.read_bytes()
    result = _record_case(broken, ledger, "c2", 1, "PASS")
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert ledger.read_bytes() == before


# --------------------------------------------------------------------------- ledger token


def _go_token(tmp_path: Path, suite: Path) -> str:
    ledger = tmp_path / "ledger.json"
    _campaign(suite, ledger)
    result = _verdict(suite, ledger)
    assert result.returncode == 0, result.stdout
    return _token(result)


def test_an_imported_token_reproduces_the_go(tmp_path):
    suite = _suite(tmp_path)
    token = _go_token(tmp_path, suite)
    restored = tmp_path / "restored.json"
    _ok(_import(suite, restored, token))
    result = _verdict(suite, restored)
    assert result.returncode == 0, result.stdout
    assert _ship(result).startswith("Ship: GO (read-only scope)")


def _corrupt(token: str) -> str:
    parts = token.split(".")
    longest = max(range(len(parts)), key=lambda i: len(parts[i]))
    payload = parts[longest]
    middle = len(payload) // 2
    swapped = "A" if payload[middle] != "A" else "B"
    parts[longest] = payload[:middle] + swapped + payload[middle + 1:]
    return ".".join(parts)


def test_a_corrupted_token_is_refused(tmp_path):
    suite = _suite(tmp_path)
    token = _go_token(tmp_path, suite)
    restored = tmp_path / "restored.json"
    result = _import(suite, restored, _corrupt(token))
    assert result.returncode == 2, (result.stdout, result.stderr)


def test_a_token_from_a_different_suite_is_refused(tmp_path):
    suite = _suite(tmp_path)
    token = _go_token(tmp_path, suite)
    other = _base_suite()
    other["name"] = "a-different-suite"
    other_path = _suite(tmp_path, other, name="other.json")
    result = _import(other_path, tmp_path / "other-ledger.json", token)
    assert result.returncode == 2, (result.stdout, result.stderr)



# --------------------------------------------------------------------------- campaign binding (M2)


def test_the_ledger_commands_require_a_campaign(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    for result in (
        _declare(suite, ledger, "onboarding", 1, campaign=None),
        _record_case(suite, ledger, "c1", 1, "PASS", campaign=None),
        _verdict(suite, ledger, campaign=None),
        _import(suite, ledger, "mt1.x.y.z", campaign=None),
    ):
        assert result.returncode == 2, (result.args, result.stdout, result.stderr)
    assert not ledger.exists()


@pytest.mark.parametrize("campaign", ["A1B2", "-a1", "a b", "a_b", "a" * 33, ""])
def test_a_malformed_campaign_id_is_refused(tmp_path, campaign):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    result = _record_case(suite, ledger, "c1", 1, "PASS", campaign=campaign)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert not ledger.exists()


def test_the_longest_campaign_id_is_accepted(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _ok(_record_case(suite, ledger, "c1", 1, "PASS", campaign="a" + "-9" * 15 + "z"))


@pytest.mark.parametrize("command", ["scenario", "record-case", "record-step", "record-probe"])
def test_a_ledger_from_another_campaign_is_refused_and_unchanged(tmp_path, command):
    suite, ledger = _seeded(tmp_path)
    before = ledger.read_bytes()
    result = {
        "scenario": lambda: _declare(suite, ledger, "month-end", 1, campaign="ffff"),
        "record-case": lambda: _record_case(suite, ledger, "c2", 1, "PASS", campaign="ffff"),
        "record-step": lambda: _record_step(suite, ledger, "onboarding", 2, "PASS",
                                            campaign="ffff"),
        "record-probe": lambda: _record_probe(suite, ledger, "edge.2", "PASS",
                                              campaign="ffff"),
    }[command]()
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert ledger.read_bytes() == before


def test_a_new_campaign_cannot_reuse_a_stale_go_ledger(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    result = _verdict(suite, ledger, campaign="ffff")
    assert result.returncode == 2, result.stdout
    assert _ship(result).startswith("Ship: NO-GO"), result.stdout


def test_a_token_from_another_campaign_is_refused(tmp_path):
    suite = _suite(tmp_path)
    token = _go_token(tmp_path, suite)
    restored = tmp_path / "restored.json"
    result = _import(suite, restored, token, campaign="ffff")
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert not restored.exists()


# --------------------------------------------------------------------------- invented probes (H1)


def test_passing_invented_probes_do_not_block_go_and_are_counted(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, probes={"ordinary.1": "PASS", "refusals.2": "PASS"})
    result = _verdict(suite, ledger)
    assert result.returncode == 0, result.stdout
    assert _ship(result).startswith("Ship: GO (read-only scope)")
    assert "probe" in _coverage(result)


@pytest.mark.parametrize("verdict", ["FAIL", "UNCLEAR"])
def test_a_failing_or_unclear_invented_probe_is_no_go_and_named(tmp_path, verdict):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, probes={"ordinary.1": "PASS", "authority-3": verdict})
    result = _verdict(suite, ledger)
    _no_go(result, "authority-3")
    assert verdict in _ship(result)


# ------------------------------------------------------------------ action flags persist (M1)


def test_a_flag_on_record_persists_into_a_verdict_without_it(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _ok(_record_case(suite, ledger, "c1", 1, "PASS", "--action-case", "c2"))
    _ok(_record_case(suite, ledger, "c1", 2, "PASS"))
    for name, steps in SCENARIOS.items():
        _ok(_declare(suite, ledger, name, steps))
        for step in range(1, steps + 1):
            _ok(_record_step(suite, ledger, name, step, "PASS"))
    _no_go(_verdict(suite, ledger), "slice 2")


@pytest.mark.parametrize("command", ["scenario", "record"])
def test_a_flagged_case_cannot_be_recorded_later(tmp_path, command):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    if command == "scenario":
        _ok(_declare(suite, ledger, "onboarding", 1, "--action-case", "c2"))
    else:
        _ok(_record_case(suite, ledger, "c1", 1, "PASS", "--action-case", "c2"))
    before = ledger.read_bytes()
    result = _record_case(suite, ledger, "c2", 1, "PASS")
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert ledger.read_bytes() == before


def test_a_flag_at_verdict_persists_into_the_token(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    token = _token(_verdict(suite, ledger, "--action-case", "c2"))
    restored = tmp_path / "restored.json"
    _ok(_import(suite, restored, token))
    _no_go(_verdict(suite, restored), "slice 2")


def test_a_flag_on_import_persists(tmp_path):
    suite = _suite(tmp_path)
    token = _go_token(tmp_path, suite)
    restored = tmp_path / "restored.json"
    _ok(_import(suite, restored, token, "--action-case", "c2"))
    _no_go(_verdict(suite, restored), "slice 2")


@pytest.mark.parametrize("command", ["scenario", "import"])
def test_an_unknown_flag_is_refused_by_every_ledger_command(tmp_path, command):
    suite = _suite(tmp_path)
    ledger = tmp_path / "fresh.json"
    if command == "scenario":
        result = _declare(suite, ledger, "onboarding", 1, "--action-case", "nope")
    else:
        result = _import(suite, ledger, _go_token(tmp_path, suite), "--action-case", "nope")
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert not ledger.exists()


# ------------------------------------------------------------------ verdict refusals (L3, L5)


def test_an_unknown_flag_at_verdict_is_refused_with_a_no_go_line(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    result = _verdict(suite, ledger, "--action-case", "nope")
    assert result.returncode == 2, result.stdout
    assert _ship(result).startswith("Ship: NO-GO"), result.stdout


def _bad_cases_null(data):
    data["cases"] = None
    return json.dumps(data)


def _bad_lowercase_verdict(data):
    return json.dumps(data).replace('"PASS"', '"pass"', 1)


def _bad_empty_scenario(data):
    data["scenarios"]["empty"] = []
    return json.dumps(data)


BAD_LEDGERS = [
    ("cases-null", _bad_cases_null),
    ("unknown-verdict", _bad_lowercase_verdict),
    ("scenario-without-steps", _bad_empty_scenario),
    ("not-json", lambda data: "{not json"),
    ("not-utf8", lambda data: b"\xff\xfe\x00{"),
    ("flagged-nested-list", lambda data: json.dumps({**data, "flagged": [["c1"]]})),
]


@pytest.mark.parametrize("damage", [d for _, d in BAD_LEDGERS], ids=[i for i, _ in BAD_LEDGERS])
def test_a_badly_shaped_ledger_is_refused_with_a_no_go_line(tmp_path, damage):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    damaged = damage(json.loads(ledger.read_text()))
    if isinstance(damaged, bytes):
        ledger.write_bytes(damaged)
    else:
        ledger.write_text(damaged)
    result = _verdict(suite, ledger)
    assert "Traceback" not in result.stderr, result.stderr
    assert result.returncode == 2, result.stdout
    assert _ship(result).startswith("Ship: NO-GO"), result.stdout


# --------------------------------------------------------------------------- trailing newline (M3)


def _blob_and_file(tmp_path: Path, suffix: bytes) -> tuple[bytes, Path]:
    blob = json.dumps(_base_suite(), indent=2).encode()  # no trailing newline
    path = tmp_path / "cases.json"
    path.write_bytes(blob + suffix)
    return blob, path


def _sha_of(data: bytes) -> str:
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def test_intake_forgives_one_heredoc_newline_and_restores_the_blob(tmp_path):
    blob, path = _blob_and_file(tmp_path, b"\n")
    result, report = _intake(path, "--blob-sha", _sha_of(blob))
    assert result.returncode == 0, result.stdout
    assert report["status"] == "READY"
    assert path.read_bytes() == blob
    exact = tmp_path / "exact" / "cases.json"
    exact.parent.mkdir()
    exact.write_bytes(blob)
    assert report["suite_digest"] == _intake(exact)[1]["suite_digest"]


def test_verdict_forgives_one_heredoc_newline_and_restores_the_blob(tmp_path):
    blob, path = _blob_and_file(tmp_path, b"")
    ledger = tmp_path / "ledger.json"
    _campaign(path, ledger)
    path.write_bytes(blob + b"\n")
    result = _verdict(path, ledger, "--blob-sha", _sha_of(blob))
    assert result.returncode == 0, result.stdout
    assert _ship(result).startswith("Ship: GO (read-only scope)")
    assert path.read_bytes() == blob


@pytest.mark.parametrize("suffix", [b"\n\n", b" ", b"\r\n"], ids=["two-newlines", "space", "crlf"])
def test_any_other_difference_from_the_blob_is_malformed(tmp_path, suffix):
    blob, path = _blob_and_file(tmp_path, suffix)
    result, report = _intake(path, "--blob-sha", _sha_of(blob))
    assert result.returncode == 1, result.stdout
    assert report["status"] == "MALFORMED"
    assert path.read_bytes() == blob + suffix


# --------------------------------------------------------------------------- show (M5)


def test_show_decodes_a_token_per_case_repeat_step_and_probe(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _ok(_record_case(suite, ledger, "c1", 1, "PASS"))
    _ok(_record_case(suite, ledger, "c2", 1, "FAIL"))
    _ok(_declare(suite, ledger, "onboarding", 2))
    _ok(_record_step(suite, ledger, "onboarding", 1, "PASS"))
    _ok(_record_probe(suite, ledger, "edge.1", "UNCLEAR"))
    token = _token(_verdict(suite, ledger))
    result = _ok(_run("show", "--suite", str(suite), "--token", token))
    assert json.loads(result.stdout) == {
        "campaign": CAMPAIGN,
        "cases": {"c1": ["PASS", None], "c2": ["FAIL"]},
        "scenarios": {"onboarding": ["PASS", None]},
        "probes": {"edge.1": "UNCLEAR"},
        "flagged": [],
    }


def test_show_reports_persisted_flags(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _ok(_declare(suite, ledger, "onboarding", 1, "--action-case", "c2"))
    token = _token(_verdict(suite, ledger))
    result = _ok(_run("show", "--suite", str(suite), "--token", token))
    assert json.loads(result.stdout)["flagged"] == ["c2"]


def test_show_refuses_a_corrupted_token(tmp_path):
    suite = _suite(tmp_path)
    token = _go_token(tmp_path, suite)
    result = _run("show", "--suite", str(suite), "--token", _corrupt(token))
    assert result.returncode == 2, (result.stdout, result.stderr)


# --------------------------------------------------------------------------- repeat 1.0 (L2)


def test_an_integral_float_repeat_is_ready_and_counts_once(tmp_path):
    suite = _base_suite()
    suite["cases"][0]["repeat"] = 1.0
    jsonschema.Draft202012Validator(SCHEMA).validate(suite)
    result, report = _intake(_suite(tmp_path, suite))
    assert result.returncode == 0, result.stdout
    assert report["status"] == "READY"
    assert report["cases"][0]["repeat"] == 1
    assert report["plan"] == [{"case": "c1", "repeat": 1}, {"case": "c2", "repeat": 1}]


# ------------------------------------------------------------------ token forms and import (L4, L6)


@pytest.mark.parametrize("wrap", ["Ledger: {}", "`{}`", "`Ledger: {}`", "  {}  "],
                         ids=["ledger-prefix", "backticks", "both", "spaces"])
def test_import_accepts_the_token_as_the_report_carries_it(tmp_path, wrap):
    suite = _suite(tmp_path)
    token = _go_token(tmp_path, suite)
    restored = tmp_path / "restored.json"
    _ok(_import(suite, restored, wrap.format(token)))
    assert _verdict(suite, restored).returncode == 0


def test_import_never_overwrites_an_existing_ledger(tmp_path):
    suite = _suite(tmp_path)
    token = _go_token(tmp_path, suite)
    existing = tmp_path / "existing.json"
    _ok(_record_case(suite, existing, "c1", 1, "FAIL"))
    before = existing.read_bytes()
    result = _import(suite, existing, token)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert existing.read_bytes() == before


@pytest.mark.parametrize("name", ["on boarding", "on\tboarding", "on\nboarding"],
                         ids=["space", "tab", "newline"])
def test_a_scenario_name_with_whitespace_is_refused(tmp_path, name):
    suite, ledger = _seeded(tmp_path)
    before = ledger.read_bytes()
    result = _declare(suite, ledger, name, 1)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert ledger.read_bytes() == before


# --------------------------------------------------------------------------- criterion coverage


def test_go_names_no_uncovered_criterion(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    result = _verdict(suite, ledger)
    assert result.returncode == 0, result.stdout
    assert "uncovered criteria: none" in _coverage(result)


def test_a_criterion_whose_only_case_failed_is_uncovered(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, case_verdicts={("c2", 1): "FAIL"})
    result = _verdict(suite, ledger)
    assert result.returncode == 1, result.stdout
    assert "uncovered criteria: refuses" in _coverage(result)


def test_a_criterion_with_no_case_is_uncovered_and_no_go(tmp_path):
    suite = _base_suite()
    suite["criteria"].append({"id": "audits", "description": "It keeps an audit trail."})
    path, ledger = _suite(tmp_path, suite), tmp_path / "ledger.json"
    _campaign(path, ledger)
    result = _verdict(path, ledger)
    assert result.returncode == 1, result.stdout
    assert "uncovered criteria: audits" in _coverage(result)



def test_a_repeat_of_100_is_ready(tmp_path):
    suite = _base_suite()
    suite["cases"][0]["repeat"] = 100
    jsonschema.Draft202012Validator(SCHEMA).validate(suite)
    result, report = _intake(_suite(tmp_path, suite))
    assert result.returncode == 0, result.stdout
    assert report["status"] == "READY"
    assert len(report["plan"]) == 101


# --------------------------------------------------------------------------- planned probes (N1)


def test_a_planned_probe_never_recorded_is_not_run(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    _ok(_plan(suite, ledger, "next.1", "next-2"))
    _ok(_record_probe(suite, ledger, "next.1", "PASS"))
    result = _verdict(suite, ledger)
    _no_go(result, "NOT RUN")
    assert "next-2" in _ship(result)


def test_recording_every_planned_probe_allows_go(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _ok(_plan(suite, ledger, "next.1", "next-2"))
    _campaign(suite, ledger, probes={"next.1": "PASS", "next-2": "PASS"})
    result = _verdict(suite, ledger)
    assert result.returncode == 0, result.stdout
    assert _ship(result).startswith("Ship: GO (read-only scope)")


def test_an_unplanned_probe_can_still_be_recorded(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _ok(_plan(suite, ledger, "next.1"))
    _ok(_record_probe(suite, ledger, "extra.1", "PASS"))


PLAN_REFUSALS = [
    ("already-planned", ("planned.1",), ()),
    ("already-recorded", ("seed.1",), ()),
    ("twice-in-one-call", ("fresh.1", "fresh.1"), ()),
    ("bad-label", ("fresh 1",), ()),
    ("unknown-action-case", ("fresh.1",), ("--action-case", "nope")),
]


@pytest.mark.parametrize("labels,extra", [(labels, extra) for _, labels, extra in PLAN_REFUSALS],
                         ids=[i for i, _, _ in PLAN_REFUSALS])
def test_a_refused_plan_leaves_the_ledger_byte_identical(tmp_path, labels, extra):
    suite, ledger = _seeded(tmp_path)
    _ok(_plan(suite, ledger, "planned.1"))
    before = ledger.read_bytes()
    result = _plan(suite, ledger, *labels, extra=extra)
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert ledger.read_bytes() == before


def test_planning_against_another_campaign_is_refused(tmp_path):
    suite, ledger = _seeded(tmp_path)
    before = ledger.read_bytes()
    result = _plan(suite, ledger, "fresh.1", campaign="ffff")
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert ledger.read_bytes() == before


def test_a_flag_on_plan_persists(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    _ok(_plan(suite, ledger, "next.1", extra=("--action-case", "c2")))
    _ok(_record_probe(suite, ledger, "next.1", "PASS"))
    _no_go(_verdict(suite, ledger), "slice 2")


def test_a_planned_probe_survives_the_token(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger, probes={"next.1": "PASS"})
    _ok(_plan(suite, ledger, "next-2"))
    token = _token(_verdict(suite, ledger))
    restored = tmp_path / "restored.json"
    _ok(_import(suite, restored, token))
    result = _verdict(suite, restored)
    _no_go(result, "NOT RUN")
    assert "next-2" in _ship(result)
    shown = json.loads(_ok(_run("show", "--suite", str(suite), "--token", token)).stdout)
    assert shown["probes"] == {"next.1": "PASS", "next-2": None}


# ------------------------------------------------------------------ verdict persists flags (N2)


def test_a_flag_at_verdict_persists_in_the_same_sandbox(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _campaign(suite, ledger)
    _no_go(_verdict(suite, ledger, "--action-case", "c2"), "slice 2")
    _no_go(_verdict(suite, ledger), "slice 2")


def test_a_flag_at_verdict_creates_the_ledger_that_keeps_it(tmp_path):
    suite, ledger = _suite(tmp_path), tmp_path / "ledger.json"
    _no_go(_verdict(suite, ledger, "--action-case", "c2"), "slice 2")
    assert ledger.exists()
    before = ledger.read_bytes()
    result = _record_case(suite, ledger, "c2", 1, "PASS")
    assert result.returncode == 2, (result.stdout, result.stderr)
    assert ledger.read_bytes() == before


@pytest.mark.parametrize("extra,campaign", [
    (("--action-case", "nope"), CAMPAIGN),
    (("--action-case", "c2"), "ffff"),
], ids=["unknown-flag", "other-campaign"])
def test_a_refused_verdict_leaves_the_ledger_byte_identical(tmp_path, extra, campaign):
    suite, ledger = _seeded(tmp_path)
    before = ledger.read_bytes()
    result = _verdict(suite, ledger, *extra, campaign=campaign)
    assert result.returncode == 2, result.stdout
    assert _ship(result).startswith("Ship: NO-GO"), result.stdout
    assert ledger.read_bytes() == before
