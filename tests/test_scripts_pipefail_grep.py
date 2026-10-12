"""A `cat ... | grep -q` pipeline under `set -o pipefail` fails a correct run.

`grep -q` exits on its first match. A `cat` that is still writing when that
happens takes SIGPIPE, and `pipefail` turns the pipeline's exit status into a
failure, so a tolerance check that should pass reports a spurious one
(#4421): the first-deploy check in
`cli/scripts/e2e-cluster-approval-resume-restarts.sh` read both redirection
files through `cat | grep -qF` and rejected a refusal that did name the route.
The fix reads the files with `grep` itself, which has no writer to cut off.

These tests pin that from three sides: a source scan over every `*.sh` in
`cli/scripts/`, `scripts/` and `charts/curie/ci/` that runs under `pipefail`
refuses any pipeline from `cat` into a quiet `grep`; the scan is proven
non-vacuous against the old form on one line and split across a backslash
continuation, and silent on the direct form; and the direct form -- as the
bare command and as the tolerance condition extracted from the script itself
-- is executed under the script's own shell options with the route in the
first file and a pipe-busting 200000 bytes in the second.
"""

from __future__ import annotations

import shlex
import subprocess
from collections.abc import Iterable, Iterator
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
E2E_APPROVAL_RESUME_RESTARTS = (
    REPO_ROOT / "cli" / "scripts" / "e2e-cluster-approval-resume-restarts.sh"
)
# Decision 4: the scan covers every *.sh under these roots (recursive) whose
# source contains `pipefail`.
SCAN_ROOTS = ("cli/scripts", "scripts", "charts/curie/ci")

# Tokens that end a command or a statement: a pipeline never spans them.
_STATEMENT_BREAKS = {";", ";;", "&", "&&", "||", "|&", "(", ")", "{", "}"}
# Shell keywords, which also end a command's pipeline context.
_SHELL_KEYWORDS = {
    "if",
    "then",
    "elif",
    "else",
    "fi",
    "while",
    "until",
    "for",
    "do",
    "done",
    "case",
    "esac",
    "in",
    "time",
}
# Redirections end a stage but not a pipeline: `cat f 2>/dev/null | grep -q x`
# is still one pipeline from cat into grep.
_REDIRECTIONS = {">", "<", ">>", "<<", ">&", "<&", "&>"}


def _logical_lines(source: str) -> Iterator[tuple[int, str]]:
    """Yield (line number of the first physical line, text) with continuations joined.

    A line whose last non-blank character is a backslash continues onto the
    next one; the joined text drops the backslashes and the scan reports the
    number of the first physical line, which is where the pipeline starts.
    """
    lines = source.splitlines()
    index = 0
    while index < len(lines):
        start = index
        parts: list[str] = []
        while index < len(lines):
            parts.append(lines[index])
            index += 1
            if not lines[index - 1].rstrip().endswith("\\"):
                break
        yield (
            start + 1,
            " ".join(
                part.rstrip()[:-1] if part.rstrip().endswith("\\") else part
                for part in parts
            ),
        )


def _tokens(logical_line: str) -> list[str]:
    """Tokenize one logical line, comments stripped.

    An unlexable line yields nothing: the scan refuses the construct it can
    see, and a source line that cannot be tokenized cannot be shown to hold a
    pipeline either.
    """
    lexer = shlex.shlex(logical_line, punctuation_chars=True)
    lexer.whitespace_split = True
    lexer.commenters = "#"
    try:
        return list(lexer)
    except ValueError:
        return []


def _pipelines(tokens: list[str]) -> list[list[list[str]]]:
    """Group tokens into pipelines, each a list of stages (lists of tokens)."""
    pipelines: list[list[list[str]]] = []
    stages: list[list[str]] = []
    stage: list[str] = []
    for token in tokens:
        if token in _REDIRECTIONS:
            if stage:
                stages.append(stage)
                stage = []
        elif token == "|":
            if stage:
                stages.append(stage)
                stage = []
        elif token in _STATEMENT_BREAKS or token in _SHELL_KEYWORDS:
            if stage:
                stages.append(stage)
                stage = []
            if stages:
                pipelines.append(stages)
                stages = []
        else:
            stage.append(token)
    if stage:
        stages.append(stage)
    if stages:
        pipelines.append(stages)
    return pipelines


def _stage_arguments(stage: list[str]) -> list[str]:
    """Return [command, *arguments] with a leading `!` and assignments dropped."""
    arguments: list[str] = []
    for token in stage:
        if not arguments:
            if token == "!":
                continue
            name, separator, _ = token.partition("=")
            if separator and name.isidentifier():
                continue
        arguments.append(token)
    return arguments


def _is_quiet_grep(stage: list[str]) -> bool:
    """True when the stage runs a `grep` whose short options include `q`."""
    arguments = _stage_arguments(stage)
    if not arguments or arguments[0] != "grep":
        return False
    for token in arguments[1:]:
        if token == "--":
            return False
        if token.startswith("-") and not token.startswith("--") and "q" in token[1:]:
            return True
    return False


def _is_cat_pipe_into_quiet_grep(pipeline: list[list[str]]) -> bool:
    """True when the pipeline's first command is `cat` and some stage is quiet grep."""
    if not pipeline:
        return False
    first = _stage_arguments(pipeline[0])
    return bool(first) and first[0] == "cat" and any(
        _is_quiet_grep(stage) for stage in pipeline[1:]
    )


def _scan_source(source: str) -> set[int]:
    """Return the line numbers of every cat-into-quiet-grep pipeline in a script."""
    flagged: set[int] = set()
    for line_number, text in _logical_lines(source):
        for pipeline in _pipelines(_tokens(text)):
            if _is_cat_pipe_into_quiet_grep(pipeline):
                flagged.add(line_number)
                break
    return flagged


def _scan(roots: Iterable[Path], base: Path) -> tuple[list[str], set[str]]:
    """Scan every pipefail `*.sh` under the roots.

    Returns the findings, each `path:line` relative to `base`, and the set of
    scripts read (the non-vacuousness tests assert on that set).
    """
    findings: list[str] = []
    scanned: set[str] = set()
    for root in roots:
        if not root.is_dir():
            continue
        for path in sorted(root.rglob("*.sh")):
            if not path.is_file():
                continue
            source = path.read_text(encoding="utf8")
            if "pipefail" not in source:
                continue
            relative = path.relative_to(base).as_posix()
            scanned.add(relative)
            findings.extend(
                f"{relative}:{line}" for line in sorted(_scan_source(source))
            )
    return findings, scanned


def _real_tree_scan() -> tuple[list[str], set[str]]:
    return _scan([REPO_ROOT / root for root in SCAN_ROOTS], REPO_ROOT)


ROUTE = "alpha-route"


def _write_deploy_files(workdir: Path, route_in_first: bool) -> None:
    """Write the two redirection files the script's first deploy creates."""
    if route_in_first:
        first = f'{{"error":"route {ROUTE} is not bound"}}\n'
    else:
        first = '{"error":"route is not bound"}\n'
    (workdir / "deploy1.json").write_text(first, encoding="utf8")
    # 200000 bytes: far past a pipe's capacity, so a `cat` feeding a `grep -q`
    # that already matched is always still writing when grep exits (#4421).
    (workdir / "deploy1.err").write_bytes(b"x" * 200_000)


def _tolerance_condition(source: str) -> str:
    """Return the script's first-deploy tolerance check as one logical line."""
    matches = [
        text
        for _, text in _logical_lines(source)
        if "AGENT_OWNED" in text and "deploy1.json" in text and "deploy1.err" in text
    ]
    assert len(matches) == 1, "expected exactly one first-deploy tolerance check"
    return matches[0]


def _run_tolerance_condition(
    condition: str, workdir: Path, route: str
) -> subprocess.CompletedProcess[str]:
    """Execute the tolerance condition itself under the script's shell options.

    The extracted text ends in `; then`; the harness supplies the three
    variables it reads and completes the if statement with the failure branch
    the script takes when the tolerance check rejects the refusal.
    """
    harness = "\n".join(
        [
            "set -euo pipefail",
            "AGENT_OWNED=1",
            f"ROUTE={shlex.quote(route)}",
            f"WORKDIR={shlex.quote(str(workdir))}",
            condition,
            "exit 1",
            "fi",
            "exit 0",
        ]
    )
    return subprocess.run(
        ["bash", "-c", harness],
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )


def test_no_cat_pipe_into_quiet_grep_under_pipefail() -> None:
    """AC1: no pipefail script feeds `cat` into a quiet `grep` (#4421)."""
    findings, _ = _real_tree_scan()
    assert not findings, (
        "cat | grep -q under set -o pipefail takes SIGPIPE on a correct run (#4421): "
        + ", ".join(sorted(findings))
    )


def test_pipefail_grep_scanner_flags_the_cat_pipe_form(tmp_path: Path) -> None:
    """AC2: the scan flags the pipe form, on one line or across a continuation."""
    for root in SCAN_ROOTS:
        (tmp_path / root).mkdir(parents=True)
    script = "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            "if ! cat work/deploy1.json work/deploy1.err "
            "| grep -qF route; then echo one-line; fi",
            "if ! cat work/deploy1.json work/deploy1.err \\",
            "    | grep -qF route; then echo continued; fi",
        ]
    )
    (tmp_path / "cli" / "scripts" / "deploy.sh").write_text(script, encoding="utf8")
    findings, _ = _scan([tmp_path / root for root in SCAN_ROOTS], tmp_path)
    assert findings == [
        "cli/scripts/deploy.sh:3",
        "cli/scripts/deploy.sh:4",
    ]
    # The real-tree scan must have read at least one pipefail script from each
    # of the three directories, or it is scanning nothing.
    _, scanned = _real_tree_scan()
    for root in SCAN_ROOTS:
        assert any(path.startswith(f"{root}/") for path in scanned), (
            f"the scan read no pipefail script under {root}"
        )


def test_pipefail_grep_scanner_accepts_the_direct_form(tmp_path: Path) -> None:
    """AC2: the scan is silent on the direct form the fix installs."""
    for root in SCAN_ROOTS:
        (tmp_path / root).mkdir(parents=True)
    script = "\n".join(
        [
            "#!/usr/bin/env bash",
            "set -euo pipefail",
            'if (( ! AGENT_OWNED )) || ! grep -qF -- "$ROUTE" \\',
            '    "$WORKDIR/deploy1.json" "$WORKDIR/deploy1.err"; then fail; fi',
        ]
    )
    (tmp_path / "cli" / "scripts" / "deploy.sh").write_text(script, encoding="utf8")
    findings, _ = _scan([tmp_path / root for root in SCAN_ROOTS], tmp_path)
    assert findings == []


def test_direct_grep_matches_under_pipefail(tmp_path: Path) -> None:
    """AC3: the direct form holds under `set -euo pipefail` (#4421).

    Both halves go through the real conditions: the bare command from
    Decision 1, and the tolerance condition extracted from the script itself.
    On the unfixed script the extracted pipeline takes SIGPIPE and wrongly
    rejects a refusal that names the route.
    """
    match_dir = tmp_path / "match"
    match_dir.mkdir()
    _write_deploy_files(match_dir, route_in_first=True)
    reject_dir = tmp_path / "reject"
    reject_dir.mkdir()
    _write_deploy_files(reject_dir, route_in_first=False)

    # The bare direct form: exit 0 with the route in the first file and
    # 200000 bytes in the second, exit 1 when neither file contains the route.
    direct = subprocess.run(
        [
            "bash",
            "-c",
            'set -euo pipefail; grep -qF -- "$1" "$2" "$3"',
            "bash",
            ROUTE,
            str(match_dir / "deploy1.json"),
            str(match_dir / "deploy1.err"),
        ],
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )
    assert direct.returncode == 0, direct.stderr
    direct_absent = subprocess.run(
        [
            "bash",
            "-c",
            'set -euo pipefail; grep -qF -- "$1" "$2" "$3"',
            "bash",
            ROUTE,
            str(reject_dir / "deploy1.json"),
            str(reject_dir / "deploy1.err"),
        ],
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )
    assert direct_absent.returncode == 1, direct_absent.stdout

    # The tolerance condition extracted from the script, executed verbatim
    # under the script's own shell options: the refusal that names the route is
    # accepted, the one that does not is rejected.
    source = E2E_APPROVAL_RESUME_RESTARTS.read_text(encoding="utf8")
    condition = _tolerance_condition(source)
    accepted = _run_tolerance_condition(condition, match_dir, ROUTE)
    assert accepted.returncode == 0, (
        "the tolerance check rejected a refusal that names the route:\n"
        f"{accepted.stderr}"
    )
    rejected = _run_tolerance_condition(condition, reject_dir, ROUTE)
    assert rejected.returncode == 1, rejected.stderr
