"""Curie's host scripts must run under the bash macOS ships.

``curie dev <verb>`` runs its script with whatever ``bash`` PATH names first,
which on a stock Mac is ``/bin/bash`` 3.2, and the hand-run e2e scripts are
started the same way. Two things 3.2 refuses and bash 4.4+ accepts reached the
parity ladder's local rung: ``[[ -v NAME ]]`` is a parse error that kills the
whole script before any rung starts, and expanding an empty array under
``set -u`` is an ``unbound variable`` error at the point it runs. The executing
tests need a 3.x interpreter, so they run where one exists: macOS
``/bin/bash``, or any interpreter named by ``CURIE_TEST_BASH3``. The source scan
runs everywhere, so a Linux CI with bash 5 still refuses the constructs listed
in ``BASH4_ONLY`` in any script listed in ``HOST_SCRIPTS``. It cannot see an
empty array expanded under ``set -u``; only the executing tests catch that.

The same scripts must run on the userland macOS ships, too: it has no GNU
``timeout`` and no ``setsid``, and its BSD ``sed`` reads the argument after a
bare ``-i`` as a backup suffix. The executing tests for those sites put
stand-ins on PATH that fail the way a stock Mac's tools do, so they fail on a
Linux host as well.
"""

from __future__ import annotations

import os
import re
import shlex
import shutil
import socket
import subprocess
import time
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
LADDER_PATH = REPO_ROOT / "cli" / "scripts" / "e2e-ladder.sh"
AGENT_SKILLS_PATH = REPO_ROOT / "scripts" / "check-agent-skills.sh"
SRE_DEMO_PATH = REPO_ROOT / "cli" / "scripts" / "sre-demo-e2e.sh"
IDLE_ROUTE_PATH = (
    REPO_ROOT / "cli" / "scripts" / "e2e-cluster-idle-route-reclamation.sh"
)
MAIL_ADAPTER_PATH = REPO_ROOT / "scripts" / "e2e-mail-adapter.sh"
CLI_MAIN_PATH = REPO_ROOT / "cli" / "src" / "main.rs"
# Every script a `curie dev` verb runs, read from the verbs' dispatch.
DEV_SCRIPT_CALLS = CLI_MAIN_PATH.read_text().count("dev_script(")
DEV_SCRIPTS = [
    REPO_ROOT / path
    for path in re.findall(r'dev_script\(\s*"([^"]+\.sh)"', CLI_MAIN_PATH.read_text())
]
# Scripts a contributor runs on their own host, whose bash may be 3.2: every
# `curie dev` script, and the e2e scripts that are started by hand.
HOST_SCRIPTS = sorted({*DEV_SCRIPTS, IDLE_ROUTE_PATH, MAIL_ADAPTER_PATH})


def _bash3() -> str | None:
    for candidate in (os.environ.get("CURIE_TEST_BASH3"), "/bin/bash"):
        if not candidate or not Path(candidate).is_file():
            continue
        major = subprocess.run(
            [candidate, "-c", 'printf %s "${BASH_VERSINFO[0]}"'],
            text=True,
            capture_output=True,
            check=False,
        ).stdout
        if major == "3":
            return candidate
    return None


BASH3 = _bash3()
needs_bash3 = pytest.mark.skipif(
    BASH3 is None,
    reason="no bash 3.x here: macOS /bin/bash, or set CURIE_TEST_BASH3",
)
# A script run under 3.x must behave exactly as it does under the bash on PATH,
# which is 5.x on Linux CI, so an executing test runs under both.
EVERY_BASH = [
    pytest.param(BASH3, marks=needs_bash3, id="bash3"),
    pytest.param("bash", id="path-bash"),
]


def _shell_function(source: str, name: str, path: Path = LADDER_PATH) -> str:
    start_marker = f"{name}() {{"
    assert start_marker in source, f"{path}: missing {name}"
    start = source.index(start_marker)
    end = source.index("\n}\n", start) + len("\n}\n")
    return source[start:end]


def _top_level_block(source: str, start: str, end: str, path: Path) -> str:
    """The script's own lines from ``start`` up to, not including, ``end``."""

    assert source.count(start) == 1, f"{path}: expected one {start!r}"
    begin = source.index(start)
    assert end in source[begin:], f"{path}: missing {end!r} after {start!r}"
    return source[begin : source.index(end, begin)]


# A variable name, or a positional or special parameter.
_PARAMETER = r"([A-Za-z_][A-Za-z0-9_]*|[0-9]|[@*])"
# declare and its siblings, with any option groups before the one that matters.
_DECLARE = r"\b(declare|local|typeset|readonly)(\s+-[a-zA-Z]+)*\s+-[a-zA-Z]*"
# A builtin in command position, so `kubectl wait -n NAMESPACE` is not it.
_COMMAND = r"(^|[;&|(!{`]|\b(then|do|else|elif|if|while|until|time|command|builtin)\b)\s*"

# Constructs bash 3.2 rejects or reads differently, with the release that
# introduced each.
BASH4_ONLY = {
    r"(\[\[|&&|\|\||!)\s*(!\s*)?-v\s": "-v NAME inside [[ ]] (bash 4.2)",
    r"(\[|\btest)\s+(!\s+)?-v\s": "[ -v NAME ], always false under 3.2 (bash 4.2)",
    _DECLARE + "A": "associative arrays (bash 4.0)",
    _DECLARE + r"n\b": "namerefs (bash 4.3)",
    _DECLARE + "[lu]": "case-converting attributes (bash 4.0)",
    r"\b(mapfile|readarray)\b": "mapfile/readarray (bash 4.0)",
    r"\$\{" + _PARAMETER + r"(\[[^]]*\])?(,,?|\^\^?)\}": "case conversion (bash 4.0)",
    r"\$\{" + _PARAMETER + r"(\[[^]]*\])?@[QEPAKaULuk]\}": "${NAME@op} (bash 4.4)",
    r"\$\{[A-Za-z_][A-Za-z0-9_]*\[-[0-9]": "negative array subscript (bash 4.3)",
    r"\$\{" + _PARAMETER + r":[^}:]*:\s*-[0-9]": "negative substring length (bash 4.2)",
    r"\|&": "|& (bash 4.0)",
    r"&>>": "&>> (bash 4.0)",
    r";;&": ";;& (bash 4.0)",
    r"(^|\s)\{[A-Za-z_][A-Za-z0-9_]*\}[<>]": "{fd}> descriptor allocation (bash 4.1)",
    r"\bcoproc\b": "coproc (bash 4.0)",
    r"\bBASHPID\b": "BASHPID (bash 4.0)",
    r"\binherit_errexit\b": "inherit_errexit (bash 4.4)",
    r"\bread\b[^;|&]*\s-t\s*[0-9]*\.[0-9]": "read -t with a fraction (bash 4.0)",
    r"%\([^)]*\)T": "printf %(...)T (bash 4.2)",
    r"\{0[0-9]+\.\.[0-9]+\}": "zero-padded brace range (bash 4.0)",
    _COMMAND + r"wait\s+-n\b": "wait -n (bash 4.3)",
}


def _bash4_only_lines(source: str, name: str = "<source>") -> list[str]:
    hits = []
    for number, line in enumerate(source.splitlines(), start=1):
        if line.lstrip().startswith("#"):
            continue
        for pattern, construct in BASH4_ONLY.items():
            if re.search(pattern, line):
                hits.append(f"{name}:{number}: {construct}: {line.strip()}")
    return hits


def _script_id(path: Path) -> str:
    return str(path.relative_to(REPO_ROOT))


def test_every_curie_dev_script_is_a_host_script() -> None:
    """A dispatch the pattern cannot read would drop its script silently."""

    assert len(DEV_SCRIPTS) == DEV_SCRIPT_CALLS, (
        f"{CLI_MAIN_PATH} calls dev_script {DEV_SCRIPT_CALLS} times, but only "
        f"{len(DEV_SCRIPTS)} name a script literally"
    )
    assert {LADDER_PATH, AGENT_SKILLS_PATH, SRE_DEMO_PATH} <= set(DEV_SCRIPTS)
    missing = [_script_id(path) for path in HOST_SCRIPTS if not path.is_file()]
    assert not missing, missing


@pytest.mark.parametrize("script", HOST_SCRIPTS, ids=_script_id)
def test_script_uses_no_construct_bash_3_2_rejects(script: Path) -> None:
    hits = _bash4_only_lines(script.read_text(), _script_id(script))
    assert not hits, "bash 3.2 rejects:\n" + "\n".join(hits)


@pytest.mark.parametrize(
    "line",
    [
        "    if [[ -v LANGFUSE_OTLP_AUTH_HEADER ]]; then",
        "    declare -A seen=()",
        "    local -n ref=target",
        "    mapfile -t rows < file",
        '    case "${observed,,}" in',
        '    echo "${value@Q}"',
        "    wait -n",
        '    if ! wait -n "$pid"; then',
        "    sleep 1 & wait -n",
        "    elif wait -n; then",
        "    x=`wait -n`",
        "    command wait -n",
        "    builtin wait -n",
        "    time wait -n",
        "    if [[ ! -v NAME ]]; then",
        '    [[ -n "$a" && -v NAME ]]',
        "    if [ -v NAME ]; then",
        "    test -v NAME",
        "    declare -g -A seen=()",
        "    local -r -A seen=()",
        "    readonly -A seen=()",
        "    local -l lower",
        "    declare -u upper",
        '    echo "${1,,}"',
        '    echo "${@^^}"',
        '    echo "${rows[-1]}"',
        '    echo "${value:0:-1}"',
        "    run |& tee log",
        "    run &>> log",
        "    a) echo a ;;&",
        "    exec {fd}>file",
        "    coproc worker { sleep 1; }",
        '    echo "$BASHPID"',
        "    shopt -s inherit_errexit",
        "    read -t 0.5 line",
        "    printf '%(%s)T' -1",
        "    for n in {01..10}; do",
    ],
)
def test_the_source_scan_refuses_each_construct(line: str) -> None:
    assert _bash4_only_lines(line + "\n"), line


@pytest.mark.parametrize(
    "line",
    [
        'kubectl --context "$CONTEXT" wait -n "$NAMESPACE" --for=condition=Ready \\',
        "    grep -v pattern file",
        "    if ! grep -qv pattern file; then",
        "    local -r name=value",
        '    echo "${value:0:1}"',
        '    printf %s "${OUT}">"$file"',
        '    assert el, f"<{t}> is missing"',
        "    for n in {1..10}; do",
    ],
)
def test_the_source_scan_ignores_what_bash_3_2_accepts(line: str) -> None:
    assert not _bash4_only_lines(line + "\n"), line


def test_the_source_scan_ignores_a_comment_that_names_a_construct() -> None:
    assert not _bash4_only_lines("    # `${NAME+x}` rather than `[[ -v NAME ]]`\n")


@needs_bash3
@pytest.mark.parametrize("script", HOST_SCRIPTS, ids=_script_id)
def test_script_parses_under_bash_3_2(script: Path) -> None:
    result = subprocess.run(
        [BASH3, "-n", str(script)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, (
        f"{script} does not parse under {BASH3}, so it dies before its first "
        f"command: {result.stderr}"
    )


@needs_bash3
def test_approval_seed_reaches_every_approvals_call_under_bash_3_2(
    tmp_path: Path,
) -> None:
    """Mint, list and resolve each run with no scope arguments."""

    source = LADDER_PATH.read_text()
    seed = _shell_function(source, "seed_approval_resume_turn")
    stop = _shell_function(source, "stop_approval_seed_message")
    invocations = tmp_path / "invocations"
    stub_bin = tmp_path / "curie"
    stub_bin.write_text(
        "#!/bin/sh\n"
        'printf \'%s\\n\' "$*" >> "$STUB_INVOCATIONS"\n'
        'case "$*" in\n'
        "    *--mint-operator-principal*)\n"
        '        echo \'{"operator_principal":{"token":"tok-example"}}\' ;;\n'
        "    *--list)\n"
        '        echo \'{"pending":[{"id":"appr-1","status":"pending","route":"e2e"}]}\' ;;\n'
        "    *--resolve*) exit 1 ;;\n"
        "esac\n"
    )
    stub_bin.chmod(0o700)
    script = f"""set -euo pipefail
LIVE=0
WORKDIR="$1"
BIN="$2"
APPROVAL_SEED_CHANNEL=C0EXAMPLE1
APPROVAL_SEED_MESSAGE_PID=""
prepare_approval_seed_fixture() {{ APPROVAL_SEED_AGENT_ID=acme-approval-agent; }}
capture_stream_cursor() {{ echo 0-0; }}
{stop}
{seed}
seed_approval_resume_turn local acme-bot
"""
    result = subprocess.run(
        [BASH3, "-c", script, "bash", str(tmp_path), str(stub_bin)],
        env={**os.environ, "STUB_INVOCATIONS": str(invocations)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert "unbound variable" not in result.stderr, result.stderr
    assert invocations.is_file(), (
        f"the approval seed never reached the CLI under {BASH3}: {result.stderr}"
    )
    approvals = [
        line for line in invocations.read_text().splitlines() if " approvals " in line
    ]
    assert approvals == [
        "--json local approvals acme-approval-agent "
        "--mint-operator-principal U0EXAMPLE1",
        "--json local approvals acme-approval-agent --list",
        "--json local approvals acme-approval-agent --resolve appr-1",
    ], result.stderr
    # The stub refuses the resolve, so the seed must stop at its own refusal.
    assert result.returncode == 1
    assert "deterministic approval resolution command failed" in result.stderr


def _bash_array(source: str, name: str) -> list[str]:
    match = re.search(rf"^{name}=\(([^)]*)\)", source, re.MULTILINE)
    assert match, f"{AGENT_SKILLS_PATH}: missing {name}"
    return re.findall(r'^\s*"([^"]+)"', match.group(1), re.MULTILINE)


def _agent_skills_tree(
    tmp_path: Path, source: str | None = None
) -> tuple[Path, list[str], list[str]]:
    """A copy of the gate over a tree holding exactly the skills it lists.

    The reference validator is replaced by a uvx that answers only the pinned
    invocations the gate makes, and rejects the INVALID_SKILLS entries the way
    skills-ref does.
    """

    if source is None:
        source = AGENT_SKILLS_PATH.read_text()
    valid = _bash_array(source, "VALID_SKILLS")
    invalid = _bash_array(source, "INVALID_SKILLS")
    root = tmp_path / "repo"
    (root / "scripts").mkdir(parents=True)
    (root / "scripts" / AGENT_SKILLS_PATH.name).write_text(source)
    for skill in valid + invalid:
        (root / skill).mkdir(parents=True)
        (root / skill / "SKILL.md").write_text("---\nname: acme\n---\n")
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    uvx = bin_dir / "uvx"
    uvx.write_text(
        "#!/bin/sh\n"
        'case "$1 $3 $5" in\n'
        '    "--from --exclude-newer agentskills") ;;\n'
        '    *) echo "unexpected uvx invocation: $*" >&2; exit 97 ;;\n'
        "esac\n"
        'case "$6" in\n'
        '    --version) echo "agentskills, version 0.1.1" ;;\n'
        "    validate)\n"
        '        case ":$STUB_REJECT:" in\n'
        '            *":$7:"*) echo "Validation failed for $7"; exit 1 ;;\n'
        "        esac ;;\n"
        '    *) echo "unexpected uvx invocation: $*" >&2; exit 97 ;;\n'
        "esac\n"
    )
    uvx.chmod(0o700)
    return root, valid, invalid


def _run_agent_skills(
    interpreter: str, root: Path, invalid: list[str], tmp_path: Path
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [interpreter, str(root / "scripts" / AGENT_SKILLS_PATH.name)],
        env={
            **os.environ,
            "PATH": f"{tmp_path / 'bin'}:{os.environ['PATH']}",
            "STUB_REJECT": ":".join(invalid),
        },
        text=True,
        capture_output=True,
        check=False,
    )


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_agent_skills_gate_accepts_a_tree_its_lists_cover_exactly(
    interpreter: str, tmp_path: Path
) -> None:
    root, valid, invalid = _agent_skills_tree(tmp_path)
    result = _run_agent_skills(interpreter, root, invalid, tmp_path)
    assert result.returncode == 0, result.stderr
    assert (
        f"allowlist covers exactly the {len(valid) + len(invalid)} discovered skill(s)"
        in result.stdout
    ), result.stdout


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_agent_skills_gate_names_a_skill_neither_list_covers(
    interpreter: str, tmp_path: Path
) -> None:
    root, _, invalid = _agent_skills_tree(tmp_path)
    unlisted = "examples/acme-bot/skills/acme-bot"
    (root / unlisted).mkdir(parents=True)
    (root / unlisted / "SKILL.md").write_text("---\nname: acme-bot\n---\n")
    result = _run_agent_skills(interpreter, root, invalid, tmp_path)
    assert result.returncode == 1, result.stderr
    assert f"1 skill(s) escaped the gate: {unlisted}" in result.stderr, result.stderr
    assert "no longer exist" not in result.stderr, result.stderr


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_agent_skills_gate_names_a_listed_skill_that_is_gone(
    interpreter: str, tmp_path: Path
) -> None:
    root, valid, invalid = _agent_skills_tree(tmp_path)
    (root / valid[0] / "SKILL.md").unlink()
    result = _run_agent_skills(interpreter, root, invalid, tmp_path)
    assert result.returncode == 1, result.stderr
    assert f"1 listed skill(s) no longer exist: {valid[0]}" in result.stderr, result.stderr
    assert "escaped the gate" not in result.stderr, result.stderr


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_agent_skills_gate_matches_whole_entries_only(
    interpreter: str, tmp_path: Path
) -> None:
    """A path that shares a prefix with a listed skill is not that skill."""

    root, valid, invalid = _agent_skills_tree(tmp_path)
    shorter, longer = valid[0][:-1], valid[0] + "-extra"
    (root / valid[0] / "SKILL.md").unlink()
    for unlisted in (shorter, longer):
        (root / unlisted).mkdir(parents=True)
        (root / unlisted / "SKILL.md").write_text("---\nname: acme\n---\n")
    result = _run_agent_skills(interpreter, root, invalid, tmp_path)
    assert result.returncode == 1, result.stderr
    assert (
        f"2 skill(s) escaped the gate: {' '.join(sorted([shorter, longer]))}"
        in result.stderr
    ), result.stderr
    assert f"1 listed skill(s) no longer exist: {valid[0]}" in result.stderr, (
        result.stderr
    )


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_agent_skills_gate_matches_a_glob_character_literally(
    interpreter: str, tmp_path: Path
) -> None:
    """A skill directory named like a pattern must not match what it globs."""

    root, valid, invalid = _agent_skills_tree(tmp_path)
    globbed = valid[0][:-1] + "?"
    (root / globbed).mkdir(parents=True)
    (root / globbed / "SKILL.md").write_text("---\nname: acme\n---\n")
    result = _run_agent_skills(interpreter, root, invalid, tmp_path)
    assert result.returncode == 1, result.stderr
    assert f"1 skill(s) escaped the gate: {globbed}" in result.stderr, result.stderr


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_agent_skills_gate_runs_with_no_asserted_invalid_fixture(
    interpreter: str, tmp_path: Path
) -> None:
    """An empty INVALID_SKILLS, which 3.2 refuses to expand under set -u."""

    source, emptied = re.subn(
        r"^INVALID_SKILLS=\(.*?^\)",
        "INVALID_SKILLS=()",
        AGENT_SKILLS_PATH.read_text(),
        flags=re.MULTILINE | re.DOTALL,
    )
    assert emptied == 1, f"{AGENT_SKILLS_PATH}: missing INVALID_SKILLS"
    root, valid, invalid = _agent_skills_tree(tmp_path, source)
    assert not invalid
    result = _run_agent_skills(interpreter, root, invalid, tmp_path)
    assert result.returncode == 0, result.stderr
    assert (
        f"allowlist covers exactly the {len(valid)} discovered skill(s)" in result.stdout
    ), result.stdout
    assert "0 asserted-invalid fixture(s) still rejected." in result.stdout, (
        result.stdout
    )


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_agent_skills_gate_names_every_listed_skill_when_none_is_found(
    interpreter: str, tmp_path: Path
) -> None:
    """The discovered set is empty, which 3.2 refuses to expand under set -u."""

    root, valid, invalid = _agent_skills_tree(tmp_path)
    for skill in valid + invalid:
        (root / skill / "SKILL.md").unlink()
    result = _run_agent_skills(interpreter, root, invalid, tmp_path)
    assert result.returncode == 1, result.stderr
    listed = valid + invalid
    assert (
        f"{len(listed)} listed skill(s) no longer exist: {' '.join(listed)}"
        in result.stderr
    ), result.stderr


# One phrase from each row's own BLOCKED reason, so a reason printed under
# another row's name is caught.
SRE_DEMO_BLOCK_REASONS = {
    "read": "named every observed namespace",
    "scale": "post-approve replica change are required",
    "rearm": "a new request whose pending row is distinct",
    "configuration-denial": "MCP endpoint could not be reached",
    "rbac-ceiling": "with the platform deployment unchanged",
}


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_sre_demo_names_each_blocked_rows_own_reason(
    interpreter: str, tmp_path: Path
) -> None:
    rows = "\n".join(f"run_assertion {row} blocked" for row in SRE_DEMO_BLOCK_REASONS)
    result = subprocess.run(
        [
            interpreter,
            "-c",
            'source "$SCRIPT" prereqs >/dev/null\n'
            'evidence_dir="$HOME"\nOBSERVATION_FAILURES=0\n'
            "blocked() { return 3; }\n"
            f"{rows}\n"
            '[[ "$OBSERVATION_FAILURES" == 5 ]]',
        ],
        env={
            "PATH": os.environ["PATH"],
            "HOME": str(tmp_path),
            "CURIE_CREDENTIALS": "test-placeholder",
            "SCRIPT": str(SRE_DEMO_PATH),
        },
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    blocked = [line for line in result.stderr.splitlines() if ": BLOCKED. " in line]
    assert [line.split(":")[0] for line in blocked] == [
        f"- {row}" for row in SRE_DEMO_BLOCK_REASONS
    ], result.stderr
    for line, reason in zip(blocked, SRE_DEMO_BLOCK_REASONS.values(), strict=True):
        assert reason in line, line


@pytest.mark.parametrize("interpreter", EVERY_BASH)
@pytest.mark.parametrize(
    "routes",
    [
        [
            "curie:sandbox:route:acme-bot:1700000000.000100",
            "curie:sandbox:route:acme-bot:1700000000.000200",
        ],
        [],
    ],
    ids=["two-routes", "no-routes"],
)
def test_idle_route_reclamation_resets_every_route_the_ladder_left(
    interpreter: str, routes: list[str], tmp_path: Path
) -> None:
    block = _top_level_block(
        IDLE_ROUTE_PATH.read_text(),
        'echo "=== release routes left by the required cluster ladder ==="',
        "wait_no_resources sandboxclaims",
        IDLE_ROUTE_PATH,
    )
    (tmp_path / "routes").write_text("".join(f"{route}\n" for route in routes))
    script = f"""set -euo pipefail
WORKDIR="$1"
route_keys() {{ cat "$WORKDIR/routes"; }}
reset_thread() {{ printf 'reset %s %s\\n' "$1" "${{2#"$WORKDIR"/}}" >> "$WORKDIR/calls"; }}
wait_route_gone() {{ printf 'gone %s\\n' "$1" >> "$WORKDIR/calls"; }}
{block}"""
    result = subprocess.run(
        [interpreter, "-c", script, "bash", str(tmp_path)],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    calls = tmp_path / "calls"
    expected = []
    for index, route in enumerate(routes):
        expected += [f"reset {route} reset-preexisting-{index}.json", f"gone {route}"]
    assert (calls.read_text().splitlines() if calls.exists() else []) == expected


@pytest.mark.parametrize("interpreter", EVERY_BASH)
@pytest.mark.parametrize(
    "pods",
    [[], ["acme-filler-1", "acme-filler-2"]],
    ids=["before-fillers", "with-fillers"],
)
def test_idle_route_reclamation_cleanup_keeps_the_exit_code_and_its_workdir_goes(
    interpreter: str, pods: list[str], tmp_path: Path
) -> None:
    """A run that fails before the fillers exist still cleans up after itself."""

    cleanup = _shell_function(IDLE_ROUTE_PATH.read_text(), "cleanup", IDLE_ROUTE_PATH)
    workdir = tmp_path / "work"
    workdir.mkdir()
    kube_log = tmp_path / "kube.log"
    watches = " ".join(
        f"{kind}_WATCH_{field}=''"
        for kind in ("QUOTA", "VICTIM_CLAIM", "VICTIM_SANDBOX")
        for field in ("PID", "RAW", "ERROR")
    )
    script = f"""set -euo pipefail
WORKDIR="$1"
NAMESPACE=acme-ns
FILLER_LABEL_NAME=curie-e2e-idle-route-reclamation
{watches}
FILLER_PODS=({" ".join(pods)})
stop_pid() {{ :; }}
print_watch_diagnostic() {{ :; }}
restore_runner_ingress() {{ :; }}
delete_unrelated_valkey_keys() {{ :; }}
kube() {{ printf '%s\\n' "$*" >> "$KUBE_LOG"; }}
{cleanup}
trap cleanup EXIT
exit 3
"""
    result = subprocess.run(
        [interpreter, "-c", script, "bash", str(workdir)],
        env={**os.environ, "KUBE_LOG": str(kube_log)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 3, result.stderr
    assert not workdir.exists(), result.stderr
    assert (kube_log.read_text().splitlines() if kube_log.exists() else []) == [
        f"-n acme-ns label pod {pod} curie-e2e-idle-route-reclamation- --overwrite"
        for pod in pods
    ], result.stderr


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_mail_adapter_namespace_is_its_run_id_lowercased(
    interpreter: str,
) -> None:
    line = _top_level_block(
        MAIL_ADAPTER_PATH.read_text(),
        'NAMESPACE="curie-mail-e2e-',
        'RELEASE="curie-mail-e2e"',
        MAIL_ADAPTER_PATH,
    )
    result = subprocess.run(
        [
            interpreter,
            "-c",
            f'set -euo pipefail\nRUN_ID=20260924T120000Z-4242\n{line}printf %s "$NAMESPACE"',
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "curie-mail-e2e-20260924t120000z-4242"


@pytest.mark.parametrize("interpreter", EVERY_BASH)
@pytest.mark.parametrize(
    ("pids", "inbox_files"),
    [([], []), ([], ["inbox-a.id"]), (["4242"], ["inbox-a.id", "inbox-b.id"])],
    ids=["nothing-started", "inbox-only", "forwards-and-inboxes"],
)
def test_mail_adapter_cleanup_deletes_every_inbox_it_created(
    interpreter: str, pids: list[str], inbox_files: list[str], tmp_path: Path
) -> None:
    """A run that fails before a port-forward starts still deletes its inboxes."""

    cleanup = _shell_function(
        MAIL_ADAPTER_PATH.read_text(), "cleanup", MAIL_ADAPTER_PATH
    )
    run_tmp = tmp_path / "run"
    run_tmp.mkdir()
    log = tmp_path / "cleanup.log"
    script = f"""set -euo pipefail
TMP="$1"
CONTEXT=k8
KEEP=0
RELEASE=curie-mail-e2e
NAMESPACE=curie-mail-e2e-acme
PF_PIDS=({" ".join(pids)})
INBOX_ID_FILES=({" ".join(inbox_files)})
kill() {{ printf 'kill %s\\n' "$*" >> "$CLEANUP_LOG"; }}
delete_inbox() {{ printf 'delete %s\\n' "$1" >> "$CLEANUP_LOG"; }}
namespace_is_owned() {{ return 1; }}
{cleanup}
trap cleanup EXIT INT TERM
exit 3
"""
    result = subprocess.run(
        [interpreter, "-c", script, "bash", str(run_tmp)],
        env={**os.environ, "CLEANUP_LOG": str(log)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 3, result.stderr
    assert not run_tmp.exists(), result.stderr
    assert (log.read_text().splitlines() if log.exists() else []) == [
        *(f"kill {pid}" for pid in pids),
        *(f"delete {inbox}" for inbox in inbox_files),
    ], result.stderr


def _write_executable(path: Path, body: str) -> None:
    path.write_text(body)
    path.chmod(0o700)


def _stock_mac_userland(bin_dir: Path) -> None:
    """Stand-ins that fail the way a stock Mac's tools do (Darwin 25.6)."""

    real_sed = shutil.which("sed")
    assert real_sed, "no sed on PATH"
    for tool in ("timeout", "setsid"):
        _write_executable(
            bin_dir / tool,
            f'#!/bin/sh\necho "bash: {tool}: command not found" >&2\nexit 127\n',
        )
    _write_executable(
        bin_dir / "sed",
        "#!/bin/sh\n"
        'for argument in "$@"; do\n'
        '    case "$argument" in\n'
        "        -i|-[!-]*i)\n"
        "            echo 'sed: 1: \"...\": invalid command code f' >&2\n"
        "            exit 1 ;;\n"
        "    esac\n"
        "done\n"
        f'exec {shlex.quote(real_sed)} "$@"\n',
    )


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _top_level_assignments(source: str, names: list[str]) -> str:
    """The script's own assignments of ``names``, for a function run alone."""

    return "\n".join(
        line
        for name in names
        for line in re.findall(rf"^{name}=.*$", source, re.MULTILINE)
    )


def _pid_is_gone(pid_file: Path) -> bool:
    try:
        os.kill(int(pid_file.read_text()), 0)
    except ProcessLookupError:
        return True
    return False


GATE_PARKED_REPLY = (
    '{"status":"awaiting-approval","finalized":false,'
    '"approval_summary":"Bash: echo curie-2094-canary"}'
)


@pytest.mark.parametrize("interpreter", EVERY_BASH)
@pytest.mark.parametrize("turn", ["parks", "hangs"])
def test_live_approval_gate_case_bounds_its_turn_on_a_stock_mac(
    interpreter: str, turn: str, tmp_path: Path
) -> None:
    """The bound is the case's hang assertion, so a Mac must still have one.

    A turn that parks passes. A turn that never ends is cut off at the bound
    and named as the #1852 hang, and the bound reaches what the turn started:
    a child left holding the captured stdout would stall the case instead.
    """

    source = LADDER_PATH.read_text()
    case = _shell_function(source, "case_live_approval_gate_denies")
    state = tmp_path / "state"
    state.mkdir()
    (tmp_path / "work" / "bundle").mkdir(parents=True)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _stock_mac_userland(bin_dir)
    _write_executable(
        bin_dir / "docker",
        "#!/bin/sh\n"
        'case "$1 $2" in\n'
        '    "image inspect") ;;\n'
        '    "ps -q") echo 0123456789ab ;;\n'
        '    "ps -aq") [ -e "$STUB_STATE/down" ] || echo 0123456789ab ;;\n'
        '    "exec "*) echo CANARY_ABSENT ;;\n'
        '    *) echo "unexpected docker invocation: $*" >&2; exit 97 ;;\n'
        "esac\n",
    )
    curie = tmp_path / "curie"
    _write_executable(
        curie,
        "#!/bin/sh\n"
        'case "$*" in\n'
        '    "skill up "*) ;;\n'
        '    "--json skill message "*)\n'
        '        if [ "$STUB_TURN" = hangs ]; then\n'
        '            sleep 30 & echo "$!" > "$STUB_STATE/turn.pid"; wait\n'
        "        fi\n"
        f"        echo {shlex.quote(GATE_PARKED_REPLY)} ;;\n"
        '    "skill down") touch "$STUB_STATE/down" ;;\n'
        '    *) echo "unexpected curie invocation: $*" >&2; exit 97 ;;\n'
        "esac\n",
    )
    definitions = _top_level_assignments(
        source, ["GNU_PROCESS", "GATE_CASE_TURN_SECONDS"]
    )
    script = f"""set -euo pipefail
REPO_ROOT={shlex.quote(str(REPO_ROOT))}
{definitions}
LIVE=1
WORKDIR="$1"
BIN="$2"
RUNNER_IMAGE=curie-runner:test
GATE_CASE_NAME=curie-ladder-2094-gate-test
GATE_CASE_CREATED=0
GATE_CASE_PORT={_free_port()}
GATE_CASE_BUNDLE=""
GATE_CASE_TURN_SECONDS=1
GATE_PROMPT="Use the Bash tool."
{case}
case_live_approval_gate_denies
"""
    result = subprocess.run(
        [interpreter, "-c", script, "bash", str(tmp_path / "work"), str(curie)],
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "STUB_STATE": str(state),
            "STUB_TURN": turn,
        },
        text=True,
        capture_output=True,
        timeout=20,
        check=False,
    )
    if turn == "parks":
        assert result.returncode == 0, result.stderr
        assert "the gated turn parked" in result.stdout, result.stdout
        assert "the gated command did not run" in result.stdout, result.stdout
    else:
        assert result.returncode == 1, result.stderr
        assert "the gated turn never ended" in result.stderr, result.stderr
        assert _pid_is_gone(state / "turn.pid"), "the bound left the turn's child running"


IDLE_ROUTE_KUBECTL = """#!/bin/sh
case "$*" in
    *--watch*)
        echo "$$" > "$STUB_STATE/kubectl-watch.pid"
        case "$*" in
            *resourcequota*) echo 'ADDED|42|2|2|2' ;;
            *) echo 'ADDED|42|uid-1' ;;
        esac
        exec sleep 30 ;;
    *resourcequota*)
        echo '{"metadata":{"resourceVersion":"41"},"status":{"used":{"pods":"2"},"hard":{"pods":"2"}},"spec":{"hard":{"pods":"2"}}}' ;;
    *) echo '{"metadata":{"resourceVersion":"41","uid":"uid-1"}}' ;;
esac
"""


@pytest.mark.parametrize("interpreter", EVERY_BASH)
@pytest.mark.parametrize(
    ("watch", "event"),
    [("exact", "ADDED\t42\tuid-1"), ("quota", "ADDED\t42\t2\t2\t2")],
)
@pytest.mark.parametrize("ending", ["stopped", "bounded"])
def test_idle_route_watch_leads_its_group_and_ends_on_a_stock_mac(
    interpreter: str, watch: str, event: str, ending: str, tmp_path: Path
) -> None:
    """stop_pid signals the group `$!` names, and a watch ends at WAIT_SECONDS.

    The session is what lets stop_pid reach the watch's kubectl, and the bound
    is what ends a watch nobody stops. A stock Mac ships neither setsid nor
    timeout, so both must come from somewhere else there.
    """

    source = IDLE_ROUTE_PATH.read_text()
    functions = "".join(
        _shell_function(source, name, IDLE_ROUTE_PATH)
        for name in (
            "kube",
            "timestamp_utc",
            "stop_pid",
            "start_quota_watch",
            "start_exact_resource_watch",
        )
    )
    state = tmp_path / "state"
    state.mkdir()
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _stock_mac_userland(bin_dir)
    kubectl = tmp_path / "kubectl"
    _write_executable(kubectl, IDLE_ROUTE_KUBECTL)
    start = {
        "exact": 'pid="$(start_exact_resource_watch sandbox acme-sandbox '
        '"$W/watch" "$W/snapshot" "$W/stderr" "$W/raw")"',
        "quota": 'start_quota_watch "$W/watch" "$W/snapshot" "$W/stderr" "$W/raw"\n'
        'pid="$QUOTA_WATCH_PID"',
    }[watch]
    finish = {
        "stopped": 'stop_pid "$pid"',
        "bounded": "for _ in $(seq 1 80); do\n"
        '    kill -0 "$pid" 2>/dev/null || break\n'
        "    sleep 0.1\n"
        "done\n"
        'if kill -0 "$pid" 2>/dev/null; then\n'
        '    echo "the watch outlived WAIT_SECONDS" >&2\n'
        "    exit 1\n"
        "fi",
    }[ending]
    script = f"""set -euo pipefail
REPO_ROOT={shlex.quote(str(REPO_ROOT))}
{_top_level_assignments(source, ["GNU_PROCESS"])}
W="$1"
REAL_KUBECTL="$2"
KUBE_CONTEXT=k8
NAMESPACE=acme-ns
RESOURCE_QUOTA=acme-quota
QUOTA_RESOURCE=pods
QUOTA_FULL=2
QUOTA_HARD=2
QUOTA_WATCH_PID=""
WAIT_SECONDS={1 if ending == "bounded" else 30}
{functions}
{start}
for _ in $(seq 1 50); do
    grep -q ADDED "$W/watch" && break
    sleep 0.1
done
python3 -c 'import os, sys; print(sys.argv[1], os.getpgid(int(sys.argv[1])))' "$pid"
{finish}
"""
    result = subprocess.run(
        [interpreter, "-c", script, "bash", str(tmp_path), str(kubectl)],
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "STUB_STATE": str(state),
        },
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    pid, group = result.stdout.split()
    assert group == pid, "the watch does not lead the group stop_pid signals"
    snapshot, *events = (tmp_path / "watch").read_text().splitlines()
    assert snapshot.startswith("SNAPSHOT\t"), snapshot
    assert [line.split("\t", 1)[1] for line in events] == [event]
    assert (tmp_path / "stderr").read_text() == ""
    deadline = time.monotonic() + 3
    while not _pid_is_gone(state / "kubectl-watch.pid"):
        assert time.monotonic() < deadline, "the watch's kubectl outlived it"
        time.sleep(0.05)


IDLE_ROUTE_CALLS = {
    "run_message": (
        'run_message acme-label hello "$WORKDIR/out.json"',
        "--json cluster message hello --namespace acme-ns --release acme "
        "--listen-host 127.0.0.1 --timeout-secs 240",
        "acme-label cluster message failed",
    ),
    "reset_thread": (
        'reset_thread curie:sandbox:route:acme-bot:1700000000.000100 "$WORKDIR/out.json"',
        "--json cluster reset-thread acme-bot --thread-key acme-bot:1700000000.000100 "
        "--namespace acme-ns --release acme --yes",
        "public reset failed for curie:sandbox:route:acme-bot:1700000000.000100",
    ),
}


@pytest.mark.parametrize("interpreter", EVERY_BASH)
@pytest.mark.parametrize("outcome", ["succeeds", "fails"])
@pytest.mark.parametrize("call", list(IDLE_ROUTE_CALLS))
def test_idle_route_runs_each_bounded_command_on_a_stock_mac(
    interpreter: str, outcome: str, call: str, tmp_path: Path
) -> None:
    invocation, argv, refusal = IDLE_ROUTE_CALLS[call]
    source = IDLE_ROUTE_PATH.read_text()
    functions = "".join(
        _shell_function(source, name, IDLE_ROUTE_PATH)
        for name in ("die", "run_message", "reset_thread")
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _stock_mac_userland(bin_dir)
    curie = tmp_path / "curie"
    _write_executable(
        curie,
        "#!/bin/sh\n"
        'printf \'%s\\n\' "$*" >> "$STUB_STATE/calls"\n'
        'if [ "$STUB_OUTCOME" = fails ]; then echo "acme refusal" >&2; exit 3; fi\n'
        "echo '{\"requested\":true}'\n",
    )
    script = f"""set -euo pipefail
REPO_ROOT={shlex.quote(str(REPO_ROOT))}
{_top_level_assignments(source, ["GNU_PROCESS"])}
WORKDIR="$1"
BIN="$2"
NAMESPACE=acme-ns
RELEASE=acme
AGENT=acme-bot
ROUTE_PREFIX="curie:sandbox:route:"
MESSAGE_TIMEOUT_SECONDS=240
CURIE_E2E_LISTEN_HOST=127.0.0.1
{functions}
{invocation}
"""
    result = subprocess.run(
        [interpreter, "-c", script, "bash", str(tmp_path), str(curie)],
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "STUB_STATE": str(tmp_path),
            "STUB_OUTCOME": outcome,
        },
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    calls = tmp_path / "calls"
    assert (calls.read_text().splitlines() if calls.exists() else []) == [argv], (
        result.stderr
    )
    if outcome == "succeeds":
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode == 1, result.stderr
        assert "acme refusal" in result.stderr, result.stderr
        assert refusal in result.stderr, result.stderr


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_idle_route_preflight_accepts_a_host_without_gnu_userland(
    interpreter: str, tmp_path: Path
) -> None:
    """Only kubectl, helm and python3 are on PATH: no timeout, no setsid."""

    source = IDLE_ROUTE_PATH.read_text()
    block = _top_level_block(
        source,
        "require_command kubectl\n",
        'REAL_KUBECTL="$(command -v kubectl)"',
        IDLE_ROUTE_PATH,
    )
    functions = "".join(
        _shell_function(source, name, IDLE_ROUTE_PATH)
        for name in ("die", "require_command")
    )
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    for tool in ("kubectl", "helm"):
        _write_executable(bin_dir / tool, "#!/bin/sh\nexit 0\n")
    python3 = shutil.which("python3")
    shell = shutil.which(interpreter)
    assert python3 and shell
    (bin_dir / "python3").symlink_to(python3)
    result = subprocess.run(
        [shell, "-c", f"set -euo pipefail\n{functions}{block}echo preflight passed\n"],
        env={"PATH": str(bin_dir)},
        text=True,
        capture_output=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    assert result.stdout == "preflight passed\n"


REGISTRY_GOOD_IMAGE = "ghcr.io/acme/tempo@sha256:" + "a" * 64
REGISTRY_LOCK = (
    "connectors:\n"
    "  tempo:\n"
    f"    image: {REGISTRY_GOOD_IMAGE}\n"
    f"    source_digest: sha256:{'b' * 64}\n"
)


@pytest.mark.parametrize("interpreter", EVERY_BASH)
def test_registry_missing_case_corrupts_and_restores_the_lock_on_a_stock_mac(
    interpreter: str, tmp_path: Path
) -> None:
    """The deploy sees the unresolvable image, and the bundle gets its lock back.

    The restored lock must be the same bytes, mode and fixed mtime, with no
    backup file left beside it, because the packed bundle is what
    assert_bundle_identity compares across rungs.
    """

    case = _shell_function(
        LADDER_PATH.read_text(), "case_connector_registry_missing_cluster"
    )
    bundle = tmp_path / "work" / "bundle"
    bundle.mkdir(parents=True)
    lock = bundle / "connectors.lock.yaml"
    lock.write_text(REGISTRY_LOCK)
    lock.chmod(0o644)
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    _stock_mac_userland(bin_dir)
    _write_executable(
        bin_dir / "kubectl", f"#!/bin/sh\necho {shlex.quote(REGISTRY_GOOD_IMAGE)}\n"
    )
    curie = tmp_path / "curie"
    _write_executable(
        curie,
        "#!/bin/sh\n"
        'case "$*" in\n'
        '    "--json cluster deploy --namespace acme-ns --release acme --plugin-dir "*)\n'
        '        cp "$9/connectors.lock.yaml" "$STUB_STATE/deployed.lock.yaml"\n'
        '        echo "error: the registry could not resolve the locked image"\n'
        "        exit 2 ;;\n"
        "esac\n"
        'echo "unexpected curie invocation: $*" >&2\n'
        "exit 97\n",
    )
    script = f"""set -euo pipefail
WORKDIR="$1"
BIN="$2"
ns_rel=(--namespace acme-ns --release acme)
connector_object_name() {{ echo "$1-$2-$3"; }}
connector_image() {{ echo {shlex.quote(REGISTRY_GOOD_IMAGE)}; }}
{case}
case_connector_registry_missing_cluster acme acme-bot acme-ns
"""
    result = subprocess.run(
        [interpreter, "-c", script, "bash", str(tmp_path / "work"), str(curie)],
        env={
            **os.environ,
            "PATH": f"{bin_dir}:{os.environ['PATH']}",
            "STUB_STATE": str(tmp_path),
        },
        text=True,
        capture_output=True,
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    deployed = tmp_path / "deployed.lock.yaml"
    assert deployed.read_text() == REGISTRY_LOCK.replace(
        REGISTRY_GOOD_IMAGE, "ghcr.io/acme/tempo@sha256:" + "f" * 64
    )
    assert lock.read_text() == REGISTRY_LOCK
    assert lock.stat().st_mode & 0o777 == 0o644
    assert time.localtime(lock.stat().st_mtime)[:6] == (2000, 1, 1, 0, 0, 0)
    assert sorted(path.name for path in bundle.iterdir()) == [lock.name]
