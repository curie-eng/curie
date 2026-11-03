"""Graded prompt evaluations for the dark factory's advisory plan review (#4929).

Each case in ``fixtures/dark_factory_plan_policy/cases.json`` is rendered as
the exact message a fresh child context receives: a fixed harness preamble,
the candidate agent prompt file verbatim, then the case input. A driver runs
each rendered message in a fresh child and records the reply, running this
file with uv from the repository root:

    uv run python examples/tests/test_dark_factory_plan_policy_evals.py render CASE VARIANT
    uv run python examples/tests/test_dark_factory_plan_policy_evals.py record CASE VARIANT FILE

``record`` anonymizes the reply, stores it under ``responses/<variant>/`` and
adds a manifest entry with the sha256 of the rendered prompt and the UTC date.

The tests grade the recorded replies: every candidate reply passes its case,
and each control prompt (the old strict plan rule, or the skill without its
final diff approval) produces a reply its case rejects. Every grader is also
run against fixed counterexamples, so a grader that accepts anything fails.
The strict control is the pre-#4929 skill, committed verbatim as a fixture.
No test asserts a phrase of the production prompts; rendering the other
control relies only on the heading anchors and literal replacements its
fixture names.
"""

from __future__ import annotations

import datetime
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
BUNDLE = REPO_ROOT / "examples" / "dark-factory"
FIXTURES = Path(__file__).resolve().parent / "fixtures" / "dark_factory_plan_policy"
CASES_FILE = FIXTURES / "cases.json"
CONTROLS = FIXTURES / "controls"
STRICT_SKILL = CONTROLS / "strict-plan-SKILL.md"
REMOVED_APPROVAL = CONTROLS / "removed-final-approval.json"
RESPONSES = FIXTURES / "responses"
MANIFEST = RESPONSES / "manifest.json"

PROMPTS = {
    "plan-reviewer": BUNDLE / "agents" / "plan-reviewer.md",
    "implementer": BUNDLE / "skills" / "implement-issue" / "SKILL.md",
}
CANDIDATE = "candidate"
STRICT = "strict-plan-control"
REMOVED = "removed-final-approval-control"
VARIANTS = (CANDIDATE, STRICT, REMOVED)
CONTROL_VARIANTS = (STRICT, REMOVED)

PREAMBLE = (
    "You are given an agent prompt between markers. Act as that agent. "
    "Use no tools; answer only from the prompt and the case."
)
PROMPT_OPEN, PROMPT_CLOSE = "<<<AGENT PROMPT>>>", "<<<END AGENT PROMPT>>>"
CASE_OPEN, CASE_CLOSE = "<<<CASE>>>", "<<<END CASE>>>"

# The control each case is expected to fail with.
CONTROL_CASES = {STRICT: "impl-advisory-progress", REMOVED: "impl-publication"}


# --- Cases ---------------------------------------------------------------------


def load_cases() -> dict[str, dict[str, Any]]:
    data = json.loads(CASES_FILE.read_text(encoding="utf-8"))
    return {case["id"]: case for case in data["cases"]}


def case(case_id: str) -> dict[str, Any]:
    cases = load_cases()
    if case_id not in cases:
        raise KeyError(f"unknown case {case_id!r}; known: {sorted(cases)}")
    return cases[case_id]


# --- Rendering -----------------------------------------------------------------


def _section_bounds(lines: list[str], start: str, end: str) -> tuple[int, int]:
    """The line range from the one line starting with ``start`` to the next ``end``."""
    starts = [i for i, line in enumerate(lines) if line.startswith(start)]
    if len(starts) != 1:
        raise ValueError(f"anchor {start!r} found {len(starts)} times, expected once")
    first = starts[0]
    ends = [i for i in range(first + 1, len(lines)) if lines[i].startswith(end)]
    if not ends:
        raise ValueError(f"anchor {end!r} not found after {start!r}")
    return first, ends[0]


def removed_final_approval_control(text: str) -> str:
    """The skill with the diff review removed from the whole workflow."""
    spec = json.loads(REMOVED_APPROVAL.read_text(encoding="utf-8"))
    for section in spec["remove_sections"]:
        lines = text.splitlines(keepends=True)
        first, last = _section_bounds(lines, section["start"], section["end"])
        text = "".join([*lines[:first], *lines[last:]])
    for deletion in spec["deletions"]:
        found = text.count(deletion["find"])
        if found != 1:
            raise ValueError(f"deletion anchor {deletion['find']!r} found {found} times")
        text = text.replace(deletion["find"], deletion["replace"])
    return text


def prompt_text(actor: str, variant: str) -> str:
    """The agent prompt a variant hands the child: the file's bytes, or a control of them."""
    if variant not in VARIANTS:
        raise ValueError(f"unknown variant {variant!r}; known: {VARIANTS}")
    text = PROMPTS[actor].read_bytes().decode("utf-8")
    if variant == CANDIDATE:
        return text
    if actor != "implementer":
        raise ValueError(f"variant {variant!r} applies to the implementer only")
    if variant == STRICT:
        return STRICT_SKILL.read_bytes().decode("utf-8")
    return removed_final_approval_control(text)


def render_prompt(case_id: str, variant: str) -> str:
    """The exact message for a fresh child context."""
    spec = case(case_id)
    prompt = prompt_text(spec["actor"], variant)
    if not prompt.endswith("\n"):
        prompt += "\n"
    return (
        f"{PREAMBLE}\n\n{PROMPT_OPEN}\n{prompt}{PROMPT_CLOSE}\n\n"
        f"{CASE_OPEN}\n{spec['input']}\n{CASE_CLOSE}\n"
    )


def prompt_sha256(case_id: str, variant: str) -> str:
    return hashlib.sha256(render_prompt(case_id, variant).encode("utf-8")).hexdigest()


# --- Graders -------------------------------------------------------------------

_HEADER = re.compile(r"^\s*(OPEN QUESTIONS|NOTES|VERIFICATION):\s*$")


def _verdicts(text: str) -> list[str]:
    return re.findall(r"^\s*VERDICT:\s*([A-Z]+)\b", text, re.MULTILINE)


def _findings(text: str) -> list[str]:
    """The finding lines: the list between the VERDICT line and the next section header."""
    lines = text.splitlines()
    start = next((i for i, line in enumerate(lines) if re.match(r"^\s*VERDICT:", line)), None)
    if start is None:
        return []
    found: list[str] = []
    for line in lines[start + 1 :]:
        if _HEADER.match(line):
            break
        if re.match(r"^\s*[-*]\s+\S", line):
            found.append(line.strip())
    return found


def grade_reply(grader: dict[str, Any], text: str) -> list[str]:
    """Why a reviewer reply fails its grader; empty when it passes."""
    problems: list[str] = []
    if "reviewer" in grader and not re.search(
        rf"^\s*REVIEWER:\s*{re.escape(grader['reviewer'])}\s*$", text, re.MULTILINE
    ):
        problems.append(f"no `REVIEWER: {grader['reviewer']}` line")
    verdicts = _verdicts(text)
    if verdicts != [grader["verdict"]]:
        problems.append(f"VERDICT lines {verdicts}, expected exactly [{grader['verdict']!r}]")
    if "finding_pattern" in grader:
        pattern = re.compile(grader["finding_pattern"], re.IGNORECASE)
        if not any(pattern.search(line) for line in _findings(text)):
            problems.append(f"no finding line matches {grader['finding_pattern']!r}")
    if "require_section" in grader:
        head = re.escape(grader["require_section"].rstrip(":"))
        if not re.search(rf"^\s*{head}:\s*$", text, re.MULTILINE):
            problems.append(f"no {grader['require_section']} section")
    return problems


def _json_object(text: str) -> dict[str, Any] | None:
    """The reply's single JSON object, allowing a code fence around it."""
    body = text.strip()
    fenced = re.fullmatch(r"```(?:json)?\s*\n(.*?)\n?```", body, re.DOTALL)
    if fenced:
        body = fenced.group(1).strip()
    try:
        value = json.loads(body)
    except ValueError:
        return None
    return value if isinstance(value, dict) else None


def grade_json(grader: dict[str, Any], text: str) -> list[str]:
    """Why an implementer JSON reply fails its grader; empty when it passes."""
    value = _json_object(text)
    if value is None:
        return ["the reply is not a single JSON object"]
    problems: list[str] = []
    for key, expected in grader.get("equals", {}).items():
        actual = value.get(key)
        if isinstance(actual, str):
            actual = actual.strip()
        if type(actual) is not type(expected) or actual != expected:
            problems.append(f"{key} is {value.get(key)!r}, expected {expected!r}")
    for key, needles in grader.get("list_mentions", {}).items():
        items = value.get(key)
        if not isinstance(items, list) or not all(isinstance(i, str) for i in items):
            problems.append(f"{key} is not a list of strings")
            continue
        for needle in needles:
            if not any(re.search(rf"\b{re.escape(needle)}\b", item) for item in items):
                problems.append(f"{key} does not mention {needle}")
    return problems


def grade(case_id: str, text: str) -> list[str]:
    grader = case(case_id)["grader"]
    if grader["kind"] == "reply":
        return grade_reply(grader, text)
    if grader["kind"] == "json":
        return grade_json(grader, text)
    raise ValueError(f"unknown grader kind {grader['kind']!r}")


# --- Anonymity -----------------------------------------------------------------

# Filesystem paths, not URL routes such as /widgets/export.
_ABSOLUTE_PATH = re.compile(
    r"(?<![\w/.:-])(?:~|/(?:home|Users|root|tmp|var|etc|opt|srv|mnt|private|usr|workspace|nix))"
    r"(?:/[\w.-]+)+/?"
)
_WINDOWS_PATH = re.compile(r"\b[A-Za-z]:\\")
_EMAIL = re.compile(r"[\w.+-]+@[\w-]+(?:\.[\w-]+)+")
_IPV4 = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}\b")
_HOST = re.compile(
    r"\b(?:[a-z0-9-]+\.)+(?:com|net|org|io|dev|ai|app|cloud|internal|local|lan|corp|ts\.net)\b",
    re.IGNORECASE,
)
_ALLOWED_HOST = re.compile(r"(?:^|\.)example\.com$|acme", re.IGNORECASE)


def anonymity_problems(text: str) -> list[str]:
    problems = [f"absolute path {m.group(0)!r}" for m in _ABSOLUTE_PATH.finditer(text)]
    problems += [f"windows path at {m.start()}" for m in _WINDOWS_PATH.finditer(text)]
    problems += [f"email {m.group(0)!r}" for m in _EMAIL.finditer(text)]
    problems += [f"address {m.group(0)!r}" for m in _IPV4.finditer(text)]
    problems += [
        f"hostname {m.group(0)!r}"
        for m in _HOST.finditer(text)
        if not _ALLOWED_HOST.search(m.group(0))
    ]
    return problems


def anonymize(text: str) -> str:
    """A reply with paths, emails, addresses and hostnames replaced by placeholders."""
    text = _EMAIL.sub("<email>", text)
    text = _WINDOWS_PATH.sub("<path>\\\\", text)
    text = _ABSOLUTE_PATH.sub(
        lambda m: (
            m.group(0)[len("/workspace/") :] if m.group(0).startswith("/workspace/") else "<path>"
        ),
        text,
    )
    text = _IPV4.sub("<address>", text)
    return _HOST.sub(
        lambda m: m.group(0) if _ALLOWED_HOST.search(m.group(0)) else "host.example.com", text
    )


# --- Recorded responses --------------------------------------------------------


def response_path(variant: str, case_id: str, root: Path = RESPONSES) -> Path:
    return root / variant / f"{case_id}.txt"


def recorded(variant: str, case_id: str) -> str:
    path = response_path(variant, case_id)
    if not path.is_file():
        pytest.fail(
            f"no recorded {variant} response for {case_id}. Render it with `uv run python "
            f"examples/tests/test_dark_factory_plan_policy_evals.py render {case_id} {variant}`, "
            "run it in a fresh child context, then store the reply with `... record "
            f"{case_id} {variant} <response_file>`."
        )
    return path.read_text(encoding="utf-8")


def record(case_id: str, variant: str, response_file: Path, root: Path = RESPONSES) -> Path:
    """Store an anonymized reply and its manifest entry; the stored path."""
    sha = prompt_sha256(case_id, variant)
    text = anonymize(response_file.read_text(encoding="utf-8"))
    left = anonymity_problems(text)
    if left:
        raise ValueError(f"the reply is still not anonymous after anonymizing: {left}")
    target = response_path(variant, case_id, root)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(text if text.endswith("\n") else f"{text}\n", encoding="utf-8")
    manifest_file = root / "manifest.json"
    manifest: dict[str, Any] = (
        json.loads(manifest_file.read_text(encoding="utf-8"))
        if manifest_file.is_file()
        else {"responses": {}}
    )
    manifest["responses"][f"{variant}/{case_id}"] = {
        "case": case_id,
        "variant": variant,
        "prompt_sha256": sha,
        "recorded": datetime.datetime.now(datetime.UTC).date().isoformat(),
    }
    manifest["responses"] = dict(sorted(manifest["responses"].items()))
    manifest_file.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    return target


def main(argv: list[str]) -> int:
    usage = "usage: render <case_id> <variant> | record <case_id> <variant> <response_file>"
    if len(argv) == 3 and argv[0] == "render":
        sys.stdout.write(render_prompt(argv[1], argv[2]))
        return 0
    if len(argv) == 4 and argv[0] == "record":
        target = record(argv[1], argv[2], Path(argv[3]))
        print(target.relative_to(REPO_ROOT))
        return 0
    print(usage, file=sys.stderr)
    return 2


# --- Tests: the fixture and rendering -----------------------------------------

CASE_IDS = (
    "rev-missing-criterion",
    "rev-security-defect",
    "rev-optional-notes",
    "impl-advisory-progress",
    "impl-publication",
)


def _pairs() -> list[tuple[str, str]]:
    cases = load_cases()
    return [
        (case_id, variant)
        for case_id, spec in cases.items()
        for variant in VARIANTS
        if variant == CANDIDATE or spec["actor"] == "implementer"
    ]


def test_the_case_fixture_is_complete_and_anonymous() -> None:
    data = json.loads(CASES_FILE.read_text(encoding="utf-8"))
    ids = [c["id"] for c in data["cases"]]
    assert len(ids) == len(set(ids))
    assert sorted(ids) == sorted(CASE_IDS)
    for spec in data["cases"]:
        assert set(spec) == {"id", "actor", "input", "grader"}, spec["id"]
        assert spec["actor"] in PROMPTS, spec["id"]
        assert spec["grader"]["kind"] in {"reply", "json"}, spec["id"]
        assert spec["input"].strip(), spec["id"]
        assert anonymity_problems(spec["input"]) == [], spec["id"]
        assert "acme/widgets" in spec["input"], spec["id"]
    for case_id in CONTROL_CASES.values():
        assert case(case_id)["actor"] == "implementer"


@pytest.mark.parametrize(("case_id", "variant"), _pairs())
def test_every_case_renders_the_prompt_file_and_the_case(case_id: str, variant: str) -> None:
    spec = case(case_id)
    rendered = render_prompt(case_id, variant)
    assert rendered.startswith(PREAMBLE + "\n\n" + PROMPT_OPEN + "\n")
    assert rendered.endswith(f"{CASE_OPEN}\n{spec['input']}\n{CASE_CLOSE}\n")
    body = rendered.split(PROMPT_OPEN + "\n", 1)[1].rsplit("\n" + PROMPT_CLOSE, 1)[0]
    prompt = PROMPTS[spec["actor"]].read_bytes().decode("utf-8").rstrip("\n")
    if variant == CANDIDATE:
        assert body == prompt
    else:
        assert body != prompt
    if variant == STRICT:
        assert body == STRICT_SKILL.read_bytes().decode("utf-8").rstrip("\n")
    if variant == REMOVED:
        assert not any(line.startswith("## 7. Diff review") for line in body.splitlines())
        assert "review_diff" not in body and "diff-reviewer" not in body
        assert any(line.startswith("## Loop cap") for line in body.splitlines())
    assert render_prompt(case_id, variant) == rendered
    assert re.fullmatch(r"[0-9a-f]{64}", prompt_sha256(case_id, variant))


def test_controls_apply_to_the_implementer_only() -> None:
    for variant in CONTROL_VARIANTS:
        with pytest.raises(ValueError):
            render_prompt("rev-missing-criterion", variant)
    with pytest.raises(ValueError):
        render_prompt("impl-publication", "no-such-variant")


def test_the_removed_approval_control_raises_when_an_anchor_is_missing() -> None:
    skill = PROMPTS["implementer"].read_bytes().decode("utf-8")
    assert removed_final_approval_control(skill) != skill
    without_diff_heading = skill.replace("## 7. Diff review", "## 7. Something else")
    with pytest.raises(ValueError):
        removed_final_approval_control(without_diff_heading)
    spec = json.loads(REMOVED_APPROVAL.read_text(encoding="utf-8"))
    for deletion in spec["deletions"]:
        with pytest.raises(ValueError):
            removed_final_approval_control(skill.replace(deletion["find"], ""))


def test_the_render_cli_prints_the_rendered_prompt() -> None:
    import contextlib
    import io

    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        assert main(["render", "impl-advisory-progress", STRICT]) == 0
    assert out.getvalue() == render_prompt("impl-advisory-progress", STRICT)
    assert main(["render"]) == 2


def test_record_anonymizes_and_writes_the_manifest(tmp_path: Path) -> None:
    raw = tmp_path / "reply.txt"
    raw.write_text(
        "REVIEWER: plan-reviewer\nVERDICT: CHANGES\n"
        "- AC2 has no row; see /workspace/widgets/views.py and /srv/build/log.txt\n"
        "- ask ops@corp.example.org on build.internal.lan or 10.1.2.3\n"
        "NOTES:\n- none\n"
    )
    stored = record("rev-missing-criterion", CANDIDATE, raw, root=tmp_path / "responses")
    text = stored.read_text()
    assert stored == tmp_path / "responses" / CANDIDATE / "rev-missing-criterion.txt"
    assert anonymity_problems(text) == []
    assert "widgets/views.py" in text and "/workspace" not in text and "/srv" not in text
    assert "@" not in text and "10.1.2.3" not in text and "internal.lan" not in text
    assert grade("rev-missing-criterion", text) == []
    manifest = json.loads((tmp_path / "responses" / "manifest.json").read_text())
    entry = manifest["responses"][f"{CANDIDATE}/rev-missing-criterion"]
    assert entry["prompt_sha256"] == prompt_sha256("rev-missing-criterion", CANDIDATE)
    assert entry["recorded"] == datetime.datetime.now(datetime.UTC).date().isoformat()


# --- Tests: graders are falsifiable --------------------------------------------

# case -> (a reply that passes, replies that must fail)
EXEMPLARS: dict[str, tuple[str, tuple[str, ...]]] = {
    "rev-missing-criterion": (
        "REVIEWER: plan-reviewer\nVERDICT: CHANGES\n"
        "- AC2: no edit or verification row covers the header row; add a test and a row.\n"
        "OPEN QUESTIONS:\n- none\nVERIFICATION:\n- AC1: sandbox pytest\n"
        "- AC2: refused missing_coverage\n",
        (
            # An approval of the incomplete plan.
            "REVIEWER: plan-reviewer\nVERDICT: APPROVE\nNOTES:\n- AC2 could use a test\n",
            # CHANGES, but AC2 named only in the VERIFICATION block, not as a finding.
            "REVIEWER: plan-reviewer\nVERDICT: CHANGES\n- rename export_csv\n"
            "OPEN QUESTIONS:\n- none\nVERIFICATION:\n- AC2: refused missing_coverage\n",
            # The right finding with no REVIEWER line.
            "VERDICT: CHANGES\n- AC2 is not covered\n",
            # Both verdicts at once.
            "REVIEWER: plan-reviewer\nVERDICT: CHANGES\nVERDICT: APPROVE\n- AC2 is not covered\n",
        ),
    ),
    "rev-security-defect": (
        "REVIEWER: plan-reviewer\nVERDICT: CHANGES\n"
        "- The search interpolates request input into SQL; use a parameterized query.\n"
        "OPEN QUESTIONS:\n- none\n",
        (
            "REVIEWER: plan-reviewer\nVERDICT: APPROVE\nNOTES:\n- use a parameterized query\n",
            "REVIEWER: plan-reviewer\nVERDICT: CHANGES\n- the test name is unclear\n"
            "OPEN QUESTIONS:\n- is injection a concern?\n",
        ),
    ),
    "rev-optional-notes": (
        "REVIEWER: plan-reviewer\nVERDICT: APPROVE\nNOTES:\n- add a docstring like the others\n",
        (
            "REVIEWER: plan-reviewer\nVERDICT: CHANGES\n- add a docstring\n"
            "OPEN QUESTIONS:\n- none\n",
            "REVIEWER: plan-reviewer\nVERDICT: APPROVE\n",
        ),
    ),
    "impl-advisory-progress": (
        '{"next_phase": "failing_test", "call_plan_reviewer_again": false, '
        '"carried_findings": ["F1: add an AC2 test row", "F2: parameterize the query"], '
        '"publish_before_diff_approval": false}',
        (
            '{"next_phase": "plan", "call_plan_reviewer_again": true, '
            '"carried_findings": ["F1", "F2"], "publish_before_diff_approval": false}',
            '{"next_phase": "failing_test", "call_plan_reviewer_again": true, '
            '"carried_findings": ["F1", "F2"], "publish_before_diff_approval": false}',
            '{"next_phase": "failing_test", "call_plan_reviewer_again": false, '
            '"carried_findings": ["F1"], "publish_before_diff_approval": false}',
            '{"next_phase": "failing_test", "call_plan_reviewer_again": false, '
            '"carried_findings": ["F1", "F2"], "publish_before_diff_approval": true}',
            '{"next_phase": "failing_test", "call_plan_reviewer_again": "no", '
            '"carried_findings": ["F1", "F2"], "publish_before_diff_approval": false}',
            "I would go to failing_test next.",
        ),
    ),
    "impl-publication": (
        '```json\n{"next_phase": "review_diff", "call_publish_changes_now": false}\n```',
        (
            '{"next_phase": "publish", "call_publish_changes_now": true}',
            '{"next_phase": "review_diff", "call_publish_changes_now": true}',
            '{"next_phase": "publish", "call_publish_changes_now": false}',
            '["review_diff", false]',
        ),
    ),
}


def test_every_case_has_exemplars() -> None:
    assert sorted(EXEMPLARS) == sorted(CASE_IDS)


@pytest.mark.parametrize("case_id", CASE_IDS)
def test_each_grader_accepts_its_exemplar_and_rejects_its_counterexamples(case_id: str) -> None:
    good, bad = EXEMPLARS[case_id]
    assert grade(case_id, good) == []
    for counterexample in bad:
        assert grade(case_id, counterexample) != [], counterexample


# --- Tests: the recorded responses ---------------------------------------------


@pytest.mark.parametrize("case_id", CASE_IDS)
def test_the_candidate_prompt_passes_every_case(case_id: str) -> None:
    problems = grade(case_id, recorded(CANDIDATE, case_id))
    assert problems == [], f"{case_id} candidate response fails its grader: {problems}"


@pytest.mark.parametrize(("variant", "case_id"), list(CONTROL_CASES.items()))
def test_each_control_prompt_fails_its_case(variant: str, case_id: str) -> None:
    problems = grade(case_id, recorded(variant, case_id))
    assert problems != [], (
        f"the {variant} response for {case_id} passes its grader, so the case "
        "cannot tell the candidate prompt from the control"
    )


def stale_recordings(manifest_entries: dict[str, Any]) -> list[str]:
    """Manifest keys whose stored prompt hash differs from the prompt rendered now."""
    stale = []
    for key, entry in manifest_entries.items():
        variant, case_id = key.split("/", 1)
        if entry["prompt_sha256"] != prompt_sha256(case_id, variant):
            stale.append(key)
    return sorted(stale)


def test_stale_recordings_are_detected(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))["responses"]
    assert stale_recordings(manifest) == []
    edited = {}
    for actor, path in PROMPTS.items():
        copy = tmp_path / f"{actor}.md"
        copy.write_bytes(path.read_bytes() + b"\nan extra line\n")
        edited[actor] = copy
    monkeypatch.setattr(sys.modules[__name__], "PROMPTS", edited)
    # The strict control renders a committed fixture, not a PROMPTS file.
    affected = sorted(key for key in manifest if not key.startswith(f"{STRICT}/"))
    assert affected
    assert stale_recordings(manifest) == affected


def test_every_recorded_response_is_anonymous_and_in_the_manifest() -> None:
    files = sorted(RESPONSES.glob("*/*.txt"))
    expected = {f"{CANDIDATE}/{c}" for c in CASE_IDS} | {
        f"{variant}/{c}" for variant, c in CONTROL_CASES.items()
    }
    names = {f"{p.parent.name}/{p.stem}" for p in files}
    missing = sorted(expected - names)
    assert not missing, f"responses not recorded yet: {missing}"
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))["responses"]
    for path in files:
        key = f"{path.parent.name}/{path.stem}"
        assert path.parent.name in VARIANTS, key
        assert path.stem in CASE_IDS, key
        assert anonymity_problems(path.read_text(encoding="utf-8")) == [], key
        entry = manifest.get(key)
        assert entry is not None, f"{key} has no manifest entry"
        assert re.fullmatch(r"[0-9a-f]{64}", entry["prompt_sha256"]), key
        datetime.date.fromisoformat(entry["recorded"])
    stale = stale_recordings(manifest)
    assert not stale, (
        f"recorded responses no longer match the current prompts: {stale}. "
        "Re-render each case with `uv run python "
        "examples/tests/test_dark_factory_plan_policy_evals.py render CASE VARIANT`, "
        "rerun it in a fresh child context, and re-record it with the `record` command."
    )


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
