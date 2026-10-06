#!/usr/bin/env python3
"""The mean tester's ship gate: suite intake, verdict ledger and GO/NO-GO.

The tester's model judges each probe. Everything that turns those judgements
into a ship verdict runs here, deterministically: validating the target's
`acceptance/cases.json`, deciding which cases are eligible, recording each
verdict, and aggregating them into `GO (read-only scope)` or `NO-GO`. The
tester copies the `Ship:`, `Coverage:` and `Ledger:` lines this prints into its report and
never writes a ship verdict itself (docs/VALIDATOR.md, "Ship verdict").

Exit codes: 0 GO or READY, 1 NO-GO or not READY, 2 refused input.

Standard library only: it runs in the runner layer, from the bundle's
`runner.Dockerfile`, as `mean-tester-gate`.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import re
import sys
import zlib
from pathlib import Path
from typing import Any

EXIT_GO = 0
EXIT_NO_GO = 1
EXIT_REFUSED = 2

TOP_FIELDS = {"version", "name", "criteria", "cases"}
CRITERION_FIELDS = {"id", "description"}
CASE_FIELDS = {
    "id",
    "probe",
    "mode",
    "attachments",
    "expected_reply",
    "card_action",
    "expected_state",
    "criterion",
    "priority",
    "repeat",
}
MODES = {"read-or-ask", "action"}
CARD_ACTIONS = {None, "approve", "reject", "click-as-non-approver"}
PRIORITIES = {"P0", "P1"}
CASE_VERDICTS = ("PASS", "FAIL", "UNCLEAR")
STEP_VERDICTS = ("PASS", "FAIL", "UNCLEAR", "BLOCKED")
MAX_REPEAT = 100
MIN_SCENARIOS = 2
MAX_SCENARIOS = 4
TOKEN_PREFIX = "mt1"
CODES = {"PASS": "P", "FAIL": "F", "UNCLEAR": "U", "BLOCKED": "B"}
VERDICTS_BY_CODE = {code: verdict for verdict, code in CODES.items()}


class Refused(Exception):
    """Input the gate will not accept; exits 2 and changes nothing."""


# --- suite intake -----------------------------------------------------------


def git_blob_sha(data: bytes) -> str:
    """The SHA-1 Git names a blob by, as GitHub's contents API returns it."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


def _nonempty_str(value: Any) -> bool:
    return isinstance(value, str) and value != ""


def _check_fields(obj: Any, fields: set[str], where: str, errors: list[str]) -> bool:
    if not isinstance(obj, dict):
        errors.append(f"{where} is not an object")
        return False
    for key in sorted(set(obj) - fields):
        errors.append(f"{where} has unknown field {key!r}")
    for key in sorted(fields - set(obj)):
        errors.append(f"{where} is missing {key!r}")
    return True


def _check_case(case: Any, index: int, criteria: set[str], errors: list[str]) -> None:
    where = f"case {index}"
    if not _check_fields(case, CASE_FIELDS, where, errors):
        return
    if _nonempty_str(case.get("id")):
        where = f"case {case['id']!r}"
    else:
        errors.append(f"{where} needs a non-empty string id")
    if not _nonempty_str(case.get("probe")):
        errors.append(f"{where} needs a non-empty probe")
    if not (isinstance(case.get("mode"), str) and case["mode"] in MODES):
        errors.append(f"{where} mode must be read-or-ask or action")
    attachments = case.get("attachments")
    if not isinstance(attachments, list) or not all(_nonempty_str(a) for a in attachments):
        errors.append(f"{where} attachments must be a list of non-empty strings")
    expected = case.get("expected_reply")
    if (
        not isinstance(expected, list)
        or not expected
        or not all(_nonempty_str(e) for e in expected)
    ):
        errors.append(f"{where} expected_reply must be a non-empty list of non-empty strings")
    card_action = case.get("card_action")
    if not (card_action is None or (isinstance(card_action, str) and card_action in CARD_ACTIONS)):
        errors.append(f"{where} card_action must be null, approve, reject or click-as-non-approver")
    state = case.get("expected_state")
    if state is not None and not isinstance(state, dict):
        errors.append(f"{where} expected_state must be an object or null")
    if not (isinstance(case.get("criterion"), str) and case["criterion"] in criteria):
        errors.append(
            f"{where} names criterion {case.get('criterion')!r}, which the suite does not declare"
        )
    if not (isinstance(case.get("priority"), str) and case["priority"] in PRIORITIES):
        errors.append(f"{where} priority must be P0 or P1")
    # JSON Schema's "integer" includes a number with a zero fraction, such as 1.0.
    repeat = case.get("repeat")
    if isinstance(repeat, float) and repeat.is_integer():
        repeat = case["repeat"] = int(repeat)
    if isinstance(repeat, bool) or not isinstance(repeat, int) or repeat < 1:
        errors.append(f"{where} repeat must be a positive integer")
    elif repeat > MAX_REPEAT:
        errors.append(f"{where} repeat must be at most {MAX_REPEAT}")


def validate_suite(suite: Any) -> list[str]:
    """Every way `suite` departs from acceptance/schema.json version 1."""
    errors: list[str] = []
    if not _check_fields(suite, TOP_FIELDS, "suite", errors):
        return errors
    version = suite.get("version")
    if isinstance(version, bool) or version != 1:
        errors.append(f"unsupported version {version!r}; only 1 is supported")
    if not _nonempty_str(suite.get("name")):
        errors.append("suite needs a non-empty name")

    criteria: set[str] = set()
    raw_criteria = suite.get("criteria")
    if not isinstance(raw_criteria, list) or not raw_criteria:
        errors.append("suite needs at least one criterion")
        raw_criteria = []
    for index, criterion in enumerate(raw_criteria):
        where = f"criterion {index}"
        if not _check_fields(criterion, CRITERION_FIELDS, where, errors):
            continue
        cid = criterion.get("id")
        if not _nonempty_str(cid):
            errors.append(f"{where} needs a non-empty string id")
        elif cid in criteria:
            errors.append(f"duplicate criterion id {cid!r}")
        else:
            criteria.add(cid)
        if not _nonempty_str(criterion.get("description")):
            errors.append(f"{where} needs a non-empty description")

    cases = suite.get("cases")
    if not isinstance(cases, list) or not cases:
        errors.append("suite needs at least one case")
        cases = []
    seen: set[str] = set()
    for index, case in enumerate(cases):
        _check_case(case, index, criteria, errors)
        if isinstance(case, dict) and _nonempty_str(case.get("id")):
            if case["id"] in seen:
                errors.append(f"duplicate case id {case['id']!r}")
            seen.add(case["id"])
    return errors


def _action_reason(case: dict[str, Any], flagged: set[str]) -> str | None:
    if case["mode"] == "action":
        return "action case"
    if case["attachments"]:
        return "attachments"
    if case["card_action"] is not None:
        return "card action"
    if case["expected_state"] is not None:
        return "state check"
    if case["id"] in flagged:
        return "the probe asks for an action"
    return None


def intake(path: Path, blob_sha: str | None, action_cases: list[str]) -> dict[str, Any]:
    """Read and classify a suite. Raises Refused for an unknown --action-case."""
    result: dict[str, Any] = {
        "status": "MALFORMED",
        "errors": [],
        "scope": None,
        "suite_digest": None,
        "cases": [],
        "blocked_by": {},
        "plan": [],
    }
    if not path.is_file():
        result["status"] = "MISSING"
        result["errors"] = [f"no acceptance suite at {path}"]
        return result
    data = path.read_bytes()
    if blob_sha is not None:
        blob_sha = blob_sha.strip().lower()
        # A shell heredoc always ends the file with a newline, which a Git blob
        # need not have. Drop exactly that one newline when it is the only
        # difference, so the file and its digest are the blob's bytes.
        if (
            git_blob_sha(data) != blob_sha
            and data.endswith(b"\n")
            and git_blob_sha(data[:-1]) == blob_sha
        ):
            data = data[:-1]
            path.write_bytes(data)
    result["suite_digest"] = hashlib.sha256(data).hexdigest()
    if blob_sha is not None and git_blob_sha(data) != blob_sha:
        result["errors"] = [
            "the suite file does not match the Git blob it was read from "
            f"(expected blob {blob_sha}, file is {git_blob_sha(data)}); "
            "write it again byte for byte"
        ]
        return result
    try:
        suite = json.loads(data)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        result["errors"] = [f"not valid JSON: {exc}"]
        return result
    errors = validate_suite(suite)
    if errors:
        result["errors"] = errors
        return result

    ids = [case["id"] for case in suite["cases"]]
    unknown = sorted(set(action_cases) - set(ids))
    if unknown:
        raise Refused(f"--action-case names no case in the suite: {', '.join(unknown)}")
    flagged = set(action_cases)

    result["status"] = "READY"
    cases = []
    reasons: dict[str, str] = {}
    for case in suite["cases"]:
        blocked = _action_reason(case, flagged)
        if blocked is not None:
            reasons[case["id"]] = blocked
        cases.append(
            {
                "id": case["id"],
                "criterion": case["criterion"],
                "priority": case["priority"],
                "repeat": case["repeat"],
                "eligible": blocked is None,
                "blocked": None if blocked is None else "slice 2",
            }
        )
    result["cases"] = cases
    result["blocked_by"] = reasons
    result["scope"] = "action" if any(not c["eligible"] for c in cases) else "read-only"
    result["plan"] = [
        {"case": c["id"], "repeat": r}
        for c in cases
        if c["eligible"]
        for r in range(1, c["repeat"] + 1)
    ]
    result["criteria"] = [c["id"] for c in suite["criteria"]]
    return result


# --- ledger -----------------------------------------------------------------
#
# The ledger holds what the tester recorded for one campaign: each fixed case
# repeat, each declared scenario step, each invented probe, and every case it
# ever flagged as asking for an action. It is bound to the suite's digest and
# the campaign id, so a stale ledger from another campaign or suite is refused
# rather than silently reused.

NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
CAMPAIGN = re.compile(r"[a-z0-9][a-z0-9-]{0,31}")


def _empty_ledger(digest: str, campaign: str) -> dict[str, Any]:
    return {
        "suite": digest,
        "campaign": campaign,
        "flagged": [],
        "cases": {},
        "scenarios": {},
        "probes": {},
    }


def _check_ledger(ledger: Any, info: dict[str, Any], campaign: str) -> dict[str, Any]:
    """Refuse a ledger that is not exactly what this gate writes for `campaign`."""
    if not isinstance(ledger, dict) or set(ledger) != set(_empty_ledger("", "")):
        raise Refused("the ledger does not have the gate's shape")
    if ledger["suite"] != info["suite_digest"]:
        raise Refused("the ledger belongs to a different suite")
    if ledger["campaign"] != campaign:
        raise Refused(
            f"the ledger belongs to campaign {ledger['campaign']!r}, not {campaign!r}; "
            "a new campaign starts from no ledger"
        )
    repeats = {c["id"]: c["repeat"] for c in info["cases"]}
    flagged = ledger["flagged"]
    if not isinstance(flagged, list) or not all(
        isinstance(f, str) and f in repeats for f in flagged
    ):
        raise Refused("the ledger flags a case the suite does not have")
    cases = ledger["cases"]
    if not isinstance(cases, dict):
        raise Refused("the ledger's cases are not an object")
    for cid, row in cases.items():
        if cid not in repeats or not isinstance(row, list) or len(row) != repeats[cid]:
            raise Refused(f"the ledger's record for case {cid!r} does not fit the suite")
        if not all(v is None or v in CASE_VERDICTS for v in row):
            raise Refused(f"the ledger's record for case {cid!r} has an unknown verdict")
    scenarios = ledger["scenarios"]
    if not isinstance(scenarios, dict):
        raise Refused("the ledger's scenarios are not an object")
    for name, steps in scenarios.items():
        if not isinstance(name, str) or not NAME.fullmatch(name):
            raise Refused(f"the ledger has a bad scenario name {name!r}")
        if not isinstance(steps, list) or not steps:
            raise Refused(f"scenario {name!r} in the ledger has no steps")
        if not all(v is None or v in STEP_VERDICTS for v in steps):
            raise Refused(f"scenario {name!r} in the ledger has an unknown verdict")
    probes = ledger["probes"]
    if not isinstance(probes, dict):
        raise Refused("the ledger's probes are not an object")
    for label, verdict in probes.items():
        if (
            not isinstance(label, str)
            or not NAME.fullmatch(label)
            or not (verdict is None or verdict in CASE_VERDICTS)
        ):
            raise Refused(f"the ledger has a bad probe record {label!r}")
    return ledger


def load_ledger(path: Path, info: dict[str, Any], campaign: str) -> dict[str, Any]:
    if not path.exists():
        return _empty_ledger(info["suite_digest"], campaign)
    try:
        ledger = json.loads(path.read_text())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise Refused(f"cannot read the ledger {path}: {exc}") from exc
    return _check_ledger(ledger, info, campaign)


def save_ledger(path: Path, ledger: dict[str, Any]) -> None:
    path.write_text(json.dumps(ledger, indent=1, sort_keys=True) + "\n")


def _ready(info: dict[str, Any]) -> dict[str, Any]:
    if info["status"] != "READY":
        raise Refused(f"the suite is {info['status']}: {'; '.join(info['errors'])}")
    return info


def _campaign(value: str) -> str:
    if not CAMPAIGN.fullmatch(value):
        raise Refused(f"campaign id {value!r} is not lowercase letters, digits and dashes")
    return value


def _open(args: argparse.Namespace) -> tuple[dict[str, Any], dict[str, Any]]:
    """The suite's intake and the campaign's ledger, with every flag ever given.

    Flags only accumulate: a case flagged once as asking for an action stays
    blocked for the rest of the campaign, whatever later commands pass.
    """
    campaign = _campaign(args.campaign)
    info = _ready(intake(args.suite, getattr(args, "blob_sha", None), args.action_case))
    ledger = load_ledger(args.ledger, info, campaign)
    ledger["flagged"] = sorted(set(ledger["flagged"]) | set(args.action_case))
    return intake(args.suite, None, ledger["flagged"]), ledger


def _codes(row: list[str | None]) -> str:
    return "".join(CODES[v] if v else "." for v in row)


def _verdicts(codes: str) -> list[str | None]:
    return [VERDICTS_BY_CODE[ch] if ch != "." else None for ch in codes]


def encode_token(ledger: dict[str, Any], cases: list[dict[str, Any]]) -> str:
    """A compact, checksummed copy of the ledger for the report's Ledger line."""
    compact = {
        "id": ledger["campaign"],
        "f": ledger["flagged"],
        "c": [_codes(_repeats(ledger, c)) for c in cases],
        "s": {name: _codes(steps) for name, steps in sorted(ledger["scenarios"].items())},
        "p": {label: CODES[v] if v else "." for label, v in sorted(ledger["probes"].items())},
    }
    raw = json.dumps(compact, separators=(",", ":"), sort_keys=True).encode()
    payload = base64.urlsafe_b64encode(zlib.compress(raw, 9)).decode().rstrip("=")
    head = f"{TOKEN_PREFIX}.{ledger['suite'][:12]}.{payload}"
    return f"{head}.{hashlib.sha256(head.encode()).hexdigest()[:8]}"


def decode_token(token: str, info: dict[str, Any]) -> dict[str, Any]:
    """The ledger a token holds, checked against the suite it claims."""
    token = token.strip().strip("`").strip()
    if token.startswith("Ledger:"):
        token = token[len("Ledger:") :].strip().strip("`").strip()
    parts = token.split(".")
    if len(parts) != 4 or parts[0] != TOKEN_PREFIX:
        raise Refused("not a mean tester ledger token")
    head = ".".join(parts[:3])
    if hashlib.sha256(head.encode()).hexdigest()[:8] != parts[3]:
        raise Refused("the ledger token's checksum does not match; it was not copied exactly")
    if parts[1] != info["suite_digest"][:12]:
        raise Refused("the ledger token was written against a different suite")
    cases = info["cases"]
    try:
        padded = parts[2] + "=" * (-len(parts[2]) % 4)
        compact = json.loads(zlib.decompress(base64.urlsafe_b64decode(padded)))
        if len(compact["c"]) != len(cases):
            raise ValueError("case count differs from the suite")
        ledger = _empty_ledger(info["suite_digest"], compact["id"])
        ledger["flagged"] = list(compact["f"])
        for case, row in zip(cases, compact["c"], strict=True):
            if any(ch != "." for ch in row):
                ledger["cases"][case["id"]] = _verdicts(row)
        ledger["scenarios"] = {name: _verdicts(row) for name, row in compact["s"].items()}
        ledger["probes"] = {
            label: VERDICTS_BY_CODE[ch] if ch != "." else None for label, ch in compact["p"].items()
        }
    except (ValueError, KeyError, TypeError, AttributeError, zlib.error) as exc:
        raise Refused(f"the ledger token does not decode: {exc}") from exc
    if not isinstance(ledger["campaign"], str):
        raise Refused("the ledger token has no campaign id")
    return _check_ledger(ledger, info, ledger["campaign"])


def _repeats(ledger: dict[str, Any], case: dict[str, Any]) -> list[str | None]:
    recorded = ledger["cases"].get(case["id"], [])
    return [recorded[i] if i < len(recorded) else None for i in range(case["repeat"])]


# --- verdict ----------------------------------------------------------------


def ship_verdict(
    info: dict[str, Any], ledger: dict[str, Any] | None, gaps: list[str]
) -> tuple[bool, list[str], str]:
    """(go, reasons, coverage line). Reasons are empty exactly when go is True."""
    if info["status"] != "READY":
        return False, [f"suite {info['status']}: {'; '.join(info['errors'])}"], "Coverage: no suite"
    assert ledger is not None
    reasons: list[str] = []
    blocked = [c for c in info["cases"] if not c["eligible"]]
    if blocked:
        reasons.append(
            "slice 2 required: action-bearing cases are BLOCKED ("
            + ", ".join(f"{c['id']} {info['blocked_by'][c['id']]}" for c in blocked)
            + "); full GO is not available"
        )

    tallies = {"PASS": 0, "FAIL": 0, "UNCLEAR": 0, "NOT RUN": 0}
    bad: dict[str, list[str]] = {"FAIL": [], "UNCLEAR": [], "NOT RUN": []}
    covered: set[str] = set()
    for case in info["cases"]:
        if not case["eligible"]:
            continue
        repeats = _repeats(ledger, case)
        for index, verdict in enumerate(repeats, start=1):
            key = verdict or "NOT RUN"
            tallies[key] += 1
            if key != "PASS":
                bad[key].append(f"{case['id']}#{index}")
        if all(v == "PASS" for v in repeats):
            covered.add(case["criterion"])
    for key in ("FAIL", "UNCLEAR", "NOT RUN"):
        if bad[key]:
            reasons.append(f"{key}: {', '.join(bad[key])}")

    # A criterion whose cases failed or were blocked is already named above;
    # one that no case tests at all is a gap in the suite itself.
    tested = {c["criterion"] for c in info["cases"]}
    untested = [c for c in info["criteria"] if c not in tested]
    if untested:
        reasons.append(f"NOT RUN: criteria with no case: {', '.join(untested)}")
    uncovered = [c for c in info["criteria"] if c not in covered]

    scenarios = ledger["scenarios"]
    if not MIN_SCENARIOS <= len(scenarios) <= MAX_SCENARIOS:
        reasons.append(
            f"{len(scenarios)} scenario sessions declared; "
            f"GO needs {MIN_SCENARIOS} to {MAX_SCENARIOS}"
        )
    step_bad: dict[str, list[str]] = {"FAIL": [], "UNCLEAR": [], "BLOCKED": [], "NOT RUN": []}
    steps_passed = steps_total = 0
    for name, steps in sorted(scenarios.items()):
        for index, verdict in enumerate(steps, start=1):
            steps_total += 1
            key = verdict or "NOT RUN"
            if key == "PASS":
                steps_passed += 1
            else:
                step_bad[key].append(f"{name}#{index}")
    for key in ("FAIL", "UNCLEAR", "BLOCKED", "NOT RUN"):
        if step_bad[key]:
            reasons.append(f"scenario steps {key}: {', '.join(step_bad[key])}")

    # Every probe the campaign invented counts too: a refusal or authority
    # probe that failed is as much a finding as a failed fixed case.
    # A probe planned but never recorded was not sent: a campaign part that
    # leaves probes for "continue" is not finished, so it is not a GO.
    probe_bad: dict[str, list[str]] = {"FAIL": [], "UNCLEAR": [], "NOT RUN": []}
    for label, verdict in sorted(ledger["probes"].items()):
        if verdict != "PASS":
            probe_bad[verdict or "NOT RUN"].append(label)
    for key in ("FAIL", "UNCLEAR", "NOT RUN"):
        if probe_bad[key]:
            reasons.append(f"probes {key}: {', '.join(probe_bad[key])}")
    for gap in gaps:
        reasons.append(f"gap: {gap}")

    eligible = sum(1 for c in info["cases"] if c["eligible"])
    probes_passed = sum(1 for v in ledger["probes"].values() if v == "PASS")
    coverage = (
        f"Coverage: {eligible}/{len(info['cases'])} cases eligible, "
        f"repeats {tallies['PASS']} PASS · {tallies['FAIL']} FAIL · "
        f"{tallies['UNCLEAR']} UNCLEAR · {tallies['NOT RUN']} NOT RUN; "
        f"criteria {len(covered)}/{len(info['criteria'])} covered "
        f"(uncovered criteria: {', '.join(uncovered) or 'none'}); "
        f"scenarios {len(scenarios)}, steps {steps_passed}/{steps_total} PASS; "
        f"probes {probes_passed}/{len(ledger['probes'])} PASS"
    )
    return not reasons, reasons, coverage


# --- commands ---------------------------------------------------------------


def cmd_intake(args: argparse.Namespace) -> int:
    info = intake(args.suite, args.blob_sha, args.action_case)
    print(json.dumps(info, indent=1))
    return EXIT_GO if info["status"] == "READY" else EXIT_NO_GO


def cmd_scenario(args: argparse.Namespace) -> int:
    info, ledger = _open(args)
    if not NAME.fullmatch(args.name):
        raise Refused("a scenario name is one word of letters, digits, '.', '_' or '-'")
    if args.steps < 1:
        raise Refused("a scenario has at least one step")
    if args.name in ledger["scenarios"]:
        raise Refused(f"scenario {args.name!r} is already declared")
    ledger["scenarios"][args.name] = [None] * args.steps
    save_ledger(args.ledger, ledger)
    print(f"declared scenario {args.name} with {args.steps} steps")
    return EXIT_GO


def _record_case(args: argparse.Namespace, info: dict[str, Any], ledger: dict[str, Any]) -> str:
    if args.step is not None or args.repeat is None:
        raise Refused("a case is recorded with --repeat, never --step")
    if args.verdict not in CASE_VERDICTS:
        raise Refused(f"a case verdict is one of {', '.join(CASE_VERDICTS)}")
    case = next((c for c in info["cases"] if c["id"] == args.case), None)
    if case is None:
        raise Refused(f"no case {args.case!r} in the suite")
    if not case["eligible"]:
        raise Refused(
            f"case {args.case!r} is BLOCKED by slice 2 "
            f"({info['blocked_by'][args.case]}); it is never sent or recorded"
        )
    if not 1 <= args.repeat <= case["repeat"]:
        raise Refused(f"case {args.case!r} has repeats 1 to {case['repeat']}")
    repeats = _repeats(ledger, case)
    if repeats[args.repeat - 1] is not None:
        raise Refused(
            f"{args.case}#{args.repeat} is already recorded as {repeats[args.repeat - 1]}"
        )
    repeats[args.repeat - 1] = args.verdict
    ledger["cases"][args.case] = repeats
    return f"{args.case}#{args.repeat}"


def _record_step(args: argparse.Namespace, ledger: dict[str, Any]) -> str:
    if args.repeat is not None or args.step is None:
        raise Refused("a scenario step is recorded with --step, never --repeat")
    if args.verdict not in STEP_VERDICTS:
        raise Refused(f"a scenario step verdict is one of {', '.join(STEP_VERDICTS)}")
    steps = ledger["scenarios"].get(args.scenario)
    if steps is None:
        raise Refused(
            f"scenario {args.scenario!r} is not declared; declare it with `scenario` first"
        )
    if not 1 <= args.step <= len(steps):
        raise Refused(f"scenario {args.scenario!r} has steps 1 to {len(steps)}")
    if steps[args.step - 1] is not None:
        raise Refused(f"{args.scenario}#{args.step} is already recorded as {steps[args.step - 1]}")
    steps[args.step - 1] = args.verdict
    return f"{args.scenario}#{args.step}"


def _record_probe(args: argparse.Namespace, ledger: dict[str, Any]) -> str:
    if args.repeat is not None or args.step is not None:
        raise Refused("an invented probe is recorded with neither --repeat nor --step")
    if args.verdict not in CASE_VERDICTS:
        raise Refused(f"a probe verdict is one of {', '.join(CASE_VERDICTS)}")
    if not NAME.fullmatch(args.probe):
        raise Refused("a probe label is one word of letters, digits, '.', '_' or '-'")
    if ledger["probes"].get(args.probe) is not None:
        raise Refused(f"probe {args.probe} is already recorded as {ledger['probes'][args.probe]}")
    label: str = args.probe
    ledger["probes"][label] = args.verdict
    return label


def cmd_plan(args: argparse.Namespace) -> int:
    _, ledger = _open(args)
    labels: list[str] = args.probe
    for label in labels:
        if not NAME.fullmatch(label):
            raise Refused(
                f"probe label {label!r} is not one word of letters, digits, '.', '_' or '-'"
            )
    duplicates = sorted(
        {x for x in labels if labels.count(x) > 1} | set(labels) & set(ledger["probes"])
    )
    if duplicates:
        raise Refused(f"probes already planned or recorded: {', '.join(duplicates)}")
    for label in labels:
        ledger["probes"][label] = None
    save_ledger(args.ledger, ledger)
    print(f"planned {len(labels)} probes")
    return EXIT_GO


def cmd_record(args: argparse.Namespace) -> int:
    info, ledger = _open(args)
    if args.case is not None:
        label = _record_case(args, info, ledger)
    elif args.scenario is not None:
        label = _record_step(args, ledger)
    else:
        label = _record_probe(args, ledger)
    save_ledger(args.ledger, ledger)
    print(f"recorded {label} {args.verdict}")
    return EXIT_GO


def cmd_verdict(args: argparse.Namespace) -> int:
    try:
        _campaign(args.campaign)
        info = intake(args.suite, args.blob_sha, args.action_case)
        ledger = None
        if info["status"] == "READY":
            info, ledger = _open(args)
            # Keep every flag this verdict was given, so a later verdict
            # without it cannot reopen a case flagged as asking for an action.
            # With no ledger and no flag there is nothing to keep, and writing
            # one would block a later `import` into this sandbox.
            if args.ledger.exists() or args.action_case:
                save_ledger(args.ledger, ledger)
    except Refused as exc:
        # The report always has a Ship line to copy, even when the gate refuses.
        print(f"Ship: NO-GO — gate refused: {exc}")
        raise
    go, reasons, coverage = ship_verdict(info, ledger, args.gap)
    if go:
        print(
            "Ship: GO (read-only scope) — every fixed case, repeat, scenario step and probe passed"
        )
    else:
        print("Ship: NO-GO — " + "; ".join(reasons))
    print(coverage)
    if ledger is not None:
        print(f"Ledger: {encode_token(ledger, info['cases'])}")
    return EXIT_GO if go else EXIT_NO_GO


def cmd_import(args: argparse.Namespace) -> int:
    campaign = _campaign(args.campaign)
    if args.ledger.exists():
        raise Refused(f"{args.ledger} already exists; import only into a sandbox without one")
    info = _ready(intake(args.suite, None, args.action_case))
    ledger = decode_token(args.token, info)
    if ledger["campaign"] != campaign:
        raise Refused(f"the token is campaign {ledger['campaign']!r}, not {campaign!r}")
    ledger["flagged"] = sorted(set(ledger["flagged"]) | set(args.action_case))
    save_ledger(args.ledger, ledger)
    print(f"imported campaign {campaign} into {args.ledger}")
    return EXIT_GO


def cmd_show(args: argparse.Namespace) -> int:
    info = _ready(intake(args.suite, None, []))
    ledger = decode_token(args.token, info)
    shown = {
        "campaign": ledger["campaign"],
        "cases": {c["id"]: _repeats(ledger, c) for c in info["cases"]},
        "scenarios": ledger["scenarios"],
        "probes": ledger["probes"],
        "flagged": ledger["flagged"],
    }
    print(json.dumps(shown, indent=1))
    return EXIT_GO


def parser() -> argparse.ArgumentParser:
    top = argparse.ArgumentParser(prog="mean-tester-gate", description=__doc__.split("\n\n")[0])
    sub = top.add_subparsers(dest="command", required=True)

    def common(p: argparse.ArgumentParser, ledger: bool) -> None:
        p.add_argument("--suite", type=Path, required=True)
        p.add_argument("--action-case", action="append", default=[])
        if ledger:
            p.add_argument("--ledger", type=Path, required=True)
            p.add_argument("--campaign", required=True)

    p = sub.add_parser("intake", help="validate and classify an acceptance suite")
    common(p, ledger=False)
    p.add_argument("--blob-sha")
    p.set_defaults(run=cmd_intake)

    p = sub.add_parser("scenario", help="declare a planned scenario session")
    common(p, ledger=True)
    p.add_argument("--name", required=True)
    p.add_argument("--steps", type=int, required=True)
    p.set_defaults(run=cmd_scenario)

    p = sub.add_parser("plan", help="declare the invented probes a campaign plans to send")
    common(p, ledger=True)
    p.add_argument("--probe", action="append", required=True)
    p.set_defaults(run=cmd_plan)

    p = sub.add_parser("record", help="record one judged probe")
    common(p, ledger=True)
    target = p.add_mutually_exclusive_group(required=True)
    target.add_argument("--case")
    target.add_argument("--scenario")
    target.add_argument("--probe")
    p.add_argument("--repeat", type=int)
    p.add_argument("--step", type=int)
    p.add_argument("--verdict", required=True)
    p.set_defaults(run=cmd_record)

    p = sub.add_parser("verdict", help="aggregate the ledger into GO or NO-GO")
    common(p, ledger=True)
    p.add_argument("--blob-sha")
    p.add_argument("--gap", action="append", default=[])
    p.set_defaults(run=cmd_verdict)

    p = sub.add_parser("import", help="restore a ledger from a report's Ledger line")
    common(p, ledger=True)
    p.add_argument("--token", required=True)
    p.set_defaults(run=cmd_import)

    p = sub.add_parser("show", help="print what a report's Ledger token holds")
    p.add_argument("--suite", type=Path, required=True)
    p.add_argument("--token", required=True)
    p.set_defaults(run=cmd_show)
    return top


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        return int(args.run(args))
    except Refused as exc:
        print(f"refused: {exc}", file=sys.stderr)
        return EXIT_REFUSED


if __name__ == "__main__":
    sys.exit(main())
