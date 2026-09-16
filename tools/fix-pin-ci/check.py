"""Apply the pull request fix pin declaration policy."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

COMMENT = re.compile(r"<!--.*?-->", re.DOTALL)
DECLARATION = re.compile(r"Fix pin: (?P<selector>\S+)")
DECLARATION_ATTEMPT = re.compile(
    r"^\s*(?:(?:[>#*_~`+-]+|\d+[.)]|\[[ x]\])\s*)*+fix[^\w\r\n]+pin\s*(?::|[-=])",
    re.IGNORECASE,
)
# The escape hatch is `Fix pin: n/a — <reason>`. An em dash, an en dash and an
# ASCII hyphen are all accepted because authors type whichever their keyboard and
# editor produce; the load bearing part is the stated reason, not the glyph.
NOT_APPLICABLE = re.compile(r"^Fix pin: (?i:n/a)(?P<rest>.*)$")
NOT_APPLICABLE_REASON = re.compile(r"^\s*[—–-]\s*(?P<reason>.*)$")
# GitHub closes an issue in the same repository only for a bare `#N` reference
# that follows a closing keyword. A `#` glued to a word character or a slash is a
# cross repository or URL reference and closes nothing here. The single optional
# colon covers GitHub's documented `Closes: #N` form.
# Deliberate, spec-directed over-inclusion (#2095): GitHub requires a closing
# keyword before EACH reference, so in `Closes #12, #13` only #12 actually
# closes. This gate treats every reference in the run as closed anyway, because
# an author who writes that intends to close both, and the gate only asks for a
# Fix pin line -- it closes nothing itself, so requiring a declaration is the
# safe side to err on.
CLOSING = re.compile(
    r"\b(?:close[sd]?|fix(?:e[sd])?|resolve[sd]?)\b\s*:?"
    r"(?P<references>(?:\s*(?:,|and)?\s*(?<![\w/])#\d+)+)",
    re.IGNORECASE,
)
REFERENCE = re.compile(r"(?<![\w/])#(?P<number>\d+)")
SELECTOR = re.compile(
    r"(?:"
    r"(?:apps|packages)/[A-Za-z0-9_-]+/tests/"
    r"(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+\.py::"
    r"(?:[A-Za-z_][A-Za-z0-9_]*::)*"
    r"test[A-Za-z0-9_]*(?:\[[A-Za-z0-9_.-]+\])?"
    r"|runner/tests/(?:[A-Za-z0-9_-]+/)*[A-Za-z0-9_-]+\.py::"
    r"(?:[A-Za-z_][A-Za-z0-9_]*::)*"
    r"test[A-Za-z0-9_]*(?:\[[A-Za-z0-9_.-]+\])?"
    r"|cli/tests/[A-Za-z0-9_-]+\.rs::[A-Za-z_][A-Za-z0-9_]*"
    r"|charts/curie/ci/[A-Za-z0-9_-]+\.sh"
    r")"
)
BUG_LABEL = "bug"
# Discovery surfaces and pin tiers share this order. A pin whose location maps
# below the closed issue's found:* label fails unless the body also carries
# `Fix pin waiver: <reason>`. Location, not prose, decides the pin tier:
# */test_live.py -> live, charts/curie/ci/* -> cluster, everything else that
# the selector grammar admits -> unit. Ladder rungs are not a declared
# selector form yet, so a found:local issue with a unit pin needs a waiver.
TIERS = ("unit", "local", "cluster", "live")
TIER_RANK = {name: index for index, name in enumerate(TIERS)}
FOUND_LABELS = {f"found:{name}": name for name in TIERS}
# GitHub issue forms render the required dropdown as a heading plus the
# selected option. Scan only that section so a narrative "not found:live" in
# the reproduction notes cannot raise the floor.
DISCOVERY_FIELD = re.compile(
    r"^### Discovery surface[ \t]*\r?\n+(?:[ \t]*\r?\n)*found:(?P<tier>unit|local|cluster|live)\b",
    re.MULTILINE,
)
WAIVER = re.compile(r"^Fix pin waiver:\s*(?P<reason>.*)$")
ISSUE_JQ = "{labels:[.labels[].name],body:.body,milestone:.milestone.title}"
MAPPING_PATH = Path(__file__).resolve().with_name("milestone-trains.json")


@dataclass(frozen=True)
class IssueRecord:
    """Labels, body, and milestone of a closed GitHub issue the gate inspects."""

    labels: list[str]
    body: str
    milestone: str | None


@dataclass(frozen=True)
class Declaration:
    """What the pull request body declared about its fix pin."""

    selector: str | None = None
    not_applicable: str | None = None


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--event", type=Path, required=True)
    parser.add_argument("--curie")
    parser.add_argument("--ref")
    parser.add_argument(
        "--needs-curie",
        action="store_true",
        help="report whether this body would invoke the curie binary",
    )
    return parser


def _event(event_path: Path) -> dict[str, object]:
    try:
        event = json.loads(event_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"could not read pull request event: {error}") from error

    if not isinstance(event, dict):
        raise ValueError("pull request event must be an object")
    return event


def _body(event: dict[str, object]) -> str:
    pull_request = event.get("pull_request")
    if not isinstance(pull_request, dict):
        return ""

    body = pull_request.get("body")
    if body is None:
        return ""
    if not isinstance(body, str):
        raise ValueError("pull request body must be a string or null")
    return body


def _repository(event: dict[str, object]) -> str | None:
    repository = event.get("repository")
    if isinstance(repository, dict):
        full_name = repository.get("full_name")
        if isinstance(full_name, str) and full_name:
            return full_name
    return os.environ.get("GITHUB_REPOSITORY") or None


def _base_ref(event: dict[str, object]) -> str:
    pull_request = event.get("pull_request")
    if not isinstance(pull_request, dict):
        raise ValueError("pull request event is missing pull_request")
    base = pull_request.get("base")
    if not isinstance(base, dict):
        raise ValueError("pull request is missing base")
    ref = base.get("ref")
    if not isinstance(ref, str) or not ref:
        raise ValueError("pull request base ref is missing")
    return ref


def _milestone_trains(path: Path = MAPPING_PATH) -> dict[str, str]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise ValueError(f"could not read {path.name}: {error}") from error
    if not isinstance(raw, dict):
        raise ValueError(f"{path.name} must be an object")

    trains = raw.get("trains")
    milestones = raw.get("milestones")
    if not isinstance(trains, dict) or not all(
        isinstance(kind, str) and isinstance(branch, str) for kind, branch in trains.items()
    ):
        raise ValueError(f"{path.name} trains must map kind names to branches")
    if not isinstance(milestones, dict) or not all(
        isinstance(name, str) and isinstance(kind, str) for name, kind in milestones.items()
    ):
        raise ValueError(f"{path.name} milestones must map titles to train kinds")

    mapping: dict[str, str] = {}
    for name, kind in milestones.items():
        branch = trains.get(kind)
        if not isinstance(branch, str) or not branch:
            raise ValueError(f"{path.name} milestone {name!r} has unknown train kind {kind!r}")
        mapping[name] = branch
    return mapping


def _declaration(body: str) -> Declaration:
    uncommented = COMMENT.sub("", body)
    attempts = [
        line for line in uncommented.splitlines() if DECLARATION_ATTEMPT.search(line)
    ]
    if not attempts:
        return Declaration()

    if len(attempts) != 1:
        raise ValueError("Fix pin declaration must be exactly one unindented selector line")

    if (excused := NOT_APPLICABLE.fullmatch(attempts[0])) is not None:
        rest = NOT_APPLICABLE_REASON.fullmatch(excused.group("rest"))
        reason = rest.group("reason").strip() if rest is not None else ""
        if not reason:
            raise ValueError(
                "Fix pin: n/a must state why, as `Fix pin: n/a - <reason>`"
            )
        return Declaration(not_applicable=reason)

    if (match := DECLARATION.fullmatch(attempts[0])) is None:
        raise ValueError("Fix pin declaration must be exactly one unindented selector line")

    selector = match.group("selector")
    if SELECTOR.fullmatch(selector) is None:
        raise ValueError(f"unsupported Fix pin selector: {selector}")
    return Declaration(selector=selector)


def _waiver(body: str) -> str | None:
    uncommented = COMMENT.sub("", body)
    reasons = [
        match.group("reason").strip()
        for line in uncommented.splitlines()
        if (match := WAIVER.fullmatch(line)) is not None
    ]
    if not reasons:
        return None
    if len(reasons) != 1:
        raise ValueError("Fix pin waiver must be exactly one unindented line")
    if not reasons[0]:
        raise ValueError("Fix pin waiver must state why, as `Fix pin waiver: <reason>`")
    return reasons[0]


def _pin_tier(selector: str) -> str:
    path = selector.split("::", 1)[0]
    filename = path.rsplit("/", 1)[-1]
    # Location only. test_live.py is the live pin the ticket names; the Python
    # CI job does not currently export CURIE_E2E_LIVE, so a live selector still
    # has to survive verify-fix-pin on its own. Until that job can run live
    # tests, a found:live bug is expected to use a lower-tier pin plus
    # `Fix pin waiver:`.
    if filename == "test_live.py":
        return "live"
    if path.startswith("charts/curie/ci/"):
        return "cluster"
    return "unit"


def _discovery_tier(labels: Sequence[str], body: str = "") -> str | None:
    found = [FOUND_LABELS[label] for label in labels if label in FOUND_LABELS]
    if (field := DISCOVERY_FIELD.search(body)) is not None:
        found.append(field.group("tier"))
    if not found:
        return None
    return max(found, key=lambda tier: TIER_RANK[tier])


def _needs_curie(body: str) -> bool:
    """True iff ``main`` would invoke the curie binary for this body."""
    try:
        return _declaration(body).selector is not None
    except ValueError:
        return False


def _report_needs_curie(event_path: Path) -> int:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if not output_path:
        print("Fix pin cargo guard error: GITHUB_OUTPUT is required", file=sys.stderr)
        return 1

    try:
        needed = _needs_curie(_body(_event(event_path)))
    except ValueError as error:
        print(f"Fix pin declaration error: {error}", file=sys.stderr)
        return 1

    line = f"needed={'true' if needed else 'false'}\n"
    try:
        with Path(output_path).open("a", encoding="utf-8") as stream:
            stream.write(line)
    except OSError as error:
        print(f"Fix pin cargo guard error: could not write GITHUB_OUTPUT: {error}", file=sys.stderr)
        return 1
    sys.stdout.write(line)
    return 0



def _closed_issues(body: str) -> list[int]:
    uncommented = COMMENT.sub("", body)
    numbers: list[int] = []
    for closure in CLOSING.finditer(uncommented):
        for reference in REFERENCE.finditer(closure.group("references")):
            number = int(reference.group("number"))
            if number not in numbers:
                numbers.append(number)
    return numbers


def _issue_record(repository: str | None, issue: int) -> IssueRecord:
    gh = shutil.which("gh")
    if gh is None:
        raise ValueError(f"gh is not on PATH, so the labels of issue #{issue} cannot be read")
    if repository is None:
        raise ValueError(
            f"no repository slug is available, so the labels of issue #{issue} cannot be read"
        )

    completed = subprocess.run(
        [
            gh,
            "api",
            f"repos/{repository}/issues/{issue}",
            "--jq",
            ISSUE_JQ,
        ],
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        detail = completed.stderr.decode("utf-8", errors="replace").strip()
        raise ValueError(f"could not read the labels of issue #{issue}: {detail}")

    try:
        payload = json.loads(completed.stdout.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError(f"could not parse the labels of issue #{issue}: {error}") from error

    if not isinstance(payload, dict):
        raise ValueError(f"could not parse the labels of issue #{issue}: not an object")
    labels = payload.get("labels")
    if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
        raise ValueError(f"could not parse the labels of issue #{issue}: not a list of names")
    body = payload.get("body")
    if body is None:
        body = ""
    if not isinstance(body, str):
        raise ValueError(f"could not parse the body of issue #{issue}: not a string")
    milestone = payload.get("milestone")
    if milestone is not None and not isinstance(milestone, str):
        raise ValueError(f"could not parse the milestone of issue #{issue}")
    title = milestone.strip() if isinstance(milestone, str) and milestone.strip() else None
    return IssueRecord(
        labels=[label for label in labels if isinstance(label, str)],
        body=body,
        milestone=title,
    )


def _closed_issue_records(
    event: dict[str, object], body: str
) -> list[tuple[int, IssueRecord]]:
    issues = _closed_issues(body)
    if not issues:
        return []
    repository = _repository(event)
    return [(issue, _issue_record(repository, issue)) for issue in issues]


def _check_milestone_trains(
    event: dict[str, object], records: list[tuple[int, IssueRecord]]
) -> None:
    if not records:
        return
    base = _base_ref(event)
    trains = _milestone_trains()
    for number, record in records:
        if record.milestone is None:
            if BUG_LABEL in record.labels:
                raise ValueError(
                    f"bug issue #{number} has no milestone; "
                    "assign a patch milestone (main) or a feature milestone (next)"
                )
            continue
        train = trains.get(record.milestone)
        if train is None:
            raise ValueError(
                f"issue #{number} milestone {record.milestone!r} is not in "
                f"{MAPPING_PATH.name}; add it as patch (main) or feature (next)"
            )
        if train != base:
            raise ValueError(
                f"issue #{number} milestone {record.milestone} maps to {train}, "
                f"but this pull request targets {base}"
            )


def _closed_bugs(event: dict[str, object], body: str) -> list[int]:
    return [
        issue
        for issue, record in _closed_issue_records(event, body)
        if BUG_LABEL in record.labels
    ]


def _required_discovery(
    event: dict[str, object], body: str
) -> str | None:
    """Highest found:* label or body field among closed bug issues."""
    found: list[str] = []
    for _issue, record in _closed_issue_records(event, body):
        if BUG_LABEL not in record.labels:
            continue
        tier = _discovery_tier(record.labels, record.body)
        if tier is not None:
            found.append(tier)
    if not found:
        return None
    return max(found, key=lambda tier: TIER_RANK[tier])


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    if arguments.needs_curie:
        return _report_needs_curie(arguments.event)
    if not arguments.curie or not arguments.ref:
        print("Fix pin invocation error: --curie and --ref are required", file=sys.stderr)
        return 2

    try:
        event = _event(arguments.event)
        body = _body(event)
        declaration = _declaration(body)
        waiver = _waiver(body)
    except ValueError as error:
        message = str(error)
        prefix = (
            "Fix pin waiver error"
            if message.startswith("Fix pin waiver")
            else "Fix pin declaration error"
        )
        print(f"{prefix}: {message}", file=sys.stderr)
        return 1

    try:
        records = _closed_issue_records(event, body)
    except ValueError as error:
        print(f"Fix pin requirement error: {error}", file=sys.stderr)
        return 1

    try:
        _check_milestone_trains(event, records)
    except ValueError as error:
        print(f"Milestone train error: {error}", file=sys.stderr)
        return 1

    if declaration.not_applicable is not None:
        print(f"SKIPPED: Fix pin declared not applicable: {declaration.not_applicable}")
        return 0

    if declaration.selector is None:
        try:
            bugs = _closed_bugs(event, body)
        except ValueError as error:
            print(f"Fix pin requirement error: {error}", file=sys.stderr)
            return 1
        if bugs:
            closed = ", ".join(f"#{issue}" for issue in bugs)
            print(
                f"Fix pin required: this pull request closes bug issue(s) {closed}. "
                "Add a `Fix pin: <selector>` line naming a test this pull request "
                "changed, or an explicit `Fix pin: n/a - <reason>` line.",
                file=sys.stderr,
            )
            return 1
        print("SKIPPED: no Fix pin declaration")
        return 0

    try:
        required = _required_discovery(event, body)
    except ValueError as error:
        print(f"Fix pin requirement error: {error}", file=sys.stderr)
        return 1
    if required is not None:
        pin = _pin_tier(declaration.selector)
        if TIER_RANK[pin] < TIER_RANK[required] and waiver is None:
            print(
                f"Fix pin tier error: selector {declaration.selector} is a {pin} "
                f"pin, which is below the discovery surface found:{required}. "
                f"Pin at found:{required} or add a `Fix pin waiver: <reason>` line.",
                file=sys.stderr,
            )
            return 1

    completed = subprocess.run(
        [arguments.curie, "dev", "verify-fix-pin", arguments.ref, declaration.selector],
        capture_output=True,
        check=False,
    )
    sys.stdout.buffer.write(completed.stdout)
    sys.stdout.buffer.flush()
    sys.stderr.buffer.write(completed.stderr)
    sys.stderr.buffer.flush()

    if completed.returncode != 0:
        return completed.returncode
    if b"PINNED" not in completed.stdout.splitlines():
        print("Fix pin verification error: verifier did not report PINNED", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
