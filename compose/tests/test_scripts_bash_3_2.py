"""Curie's host scripts must run under the bash macOS ships.

``curie dev <verb>`` runs its script with whatever ``bash`` PATH names first,
which on a stock Mac is ``/bin/bash`` 3.2, and the hand-run e2e scripts are
started the same way. Two things 3.2 refuses and bash 4.4+ accepts reached the
parity ladder's local rung: ``[[ -v NAME ]]`` is a parse error that kills the
whole script before any rung starts, and expanding an empty array under
``set -u`` is an ``unbound variable`` error at the point it runs. The executing
tests need a 3.x interpreter, so they run where one exists: macOS
``/bin/bash``, or any interpreter named by ``CURIE_TEST_BASH3``. The source scan
runs everywhere, so a Linux CI with bash 5 still refuses a known bash-4-only
construct in any script listed in ``HOST_SCRIPTS``.
"""

from __future__ import annotations

import os
import re
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
LADDER_PATH = REPO_ROOT / "cli" / "scripts" / "e2e-ladder.sh"
# Scripts a contributor runs on their own host, whose bash may be 3.2.
HOST_SCRIPTS = [LADDER_PATH]


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


def _shell_function(source: str, name: str, path: Path = LADDER_PATH) -> str:
    start_marker = f"{name}() {{"
    assert start_marker in source, f"{path}: missing {name}"
    start = source.index(start_marker)
    end = source.index("\n}\n", start) + len("\n}\n")
    return source[start:end]


# Constructs bash 3.2 rejects, with the release that introduced each.
BASH4_ONLY = {
    r"\[\[\s+-v\s": "[[ -v NAME ]] (bash 4.2)",
    r"\b(declare|local|typeset)\s+-[a-zA-Z]*A": "associative arrays (bash 4.0)",
    r"\b(declare|local|typeset)\s+-[a-zA-Z]*n\b": "namerefs (bash 4.3)",
    r"\b(mapfile|readarray)\b": "mapfile/readarray (bash 4.0)",
    r"\$\{[A-Za-z_][A-Za-z0-9_]*(\[[^]]*\])?(,,?|\^\^?)\}": "case conversion (bash 4.0)",
    r"\$\{[A-Za-z_][A-Za-z0-9_]*(\[[^]]*\])?@[QEPAKaULuk]\}": "${NAME@op} (bash 4.4)",
    r"\bwait\s+-n\b": "wait -n (bash 4.3)",
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
    ],
)
def test_the_source_scan_refuses_each_construct(line: str) -> None:
    assert _bash4_only_lines(line + "\n"), line


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
