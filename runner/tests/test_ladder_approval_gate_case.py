"""Source contracts for the ladder's live approval-gate case (#2094).

The real proof is ``case_live_approval_gate_denies`` in
``cli/scripts/e2e-ladder.sh``: it boots a runner against a real provider with
the operator gate armed on ``Bash`` and asserts the turn parks awaiting
approval instead of executing the command or spinning until the caller gives
up (#1852, #2068). That proof fires on two triggers only -- the nightly graded
ladder's live leg, and the SDK-lock PR workflow -- so a silent collapse of the
case would go unnoticed for a long time, and every way it can collapse is
quiet: a dropped ``--secret`` value never arms the gate, a missing ``LIVE``
guard runs it sealed, a missing bound wedges the run instead of failing it,
and an uninvoked case is simply dead code.

These fast tests pin the shape that makes the live case mean something. They
do not run it; they keep it from becoming a green that proves nothing.
"""

from __future__ import annotations

import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
LADDER_PATH = REPO_ROOT / "cli" / "scripts" / "e2e-ladder.sh"

CASE_NAME = "case_live_approval_gate_denies"
# The case is inserted beside the other `case_*` helpers, above the connector
# block that opens the ADR-0113 section.
CASE_END_MARKER = "# The connector rung (ADR 0113"
CANARY_PATH = "/tmp/curie-2094-canary"
# The probe answers with one of two definite tokens, so that "docker could not
# run the command" stays distinguishable from "the file is absent".
CANARY_PRESENT = "CANARY_PRESENT"
CANARY_ABSENT = "CANARY_ABSENT"
PARITY_BUNDLE = "$WORKDIR/bundle"
CASE_BUNDLE = "$WORKDIR/gate-bundle"


def _function_body(source: str, name: str, next_marker: str) -> str:
    start_marker = f"{name}() {{"
    assert start_marker in source, f"{LADDER_PATH}: missing {name}"
    start = source.index(start_marker)
    end = source.index(next_marker, start)
    return source[start:end]


def _case_body() -> str:
    return _function_body(LADDER_PATH.read_text(), CASE_NAME, CASE_END_MARKER)


def _logical_line_at(body: str, needle: str) -> str:
    """Return the whole backslash-continued command containing ``needle``.

    The `skill message` invocation is wrapped across continuation lines, so a
    per-physical-line search would not see the bound that guards it, and a
    whole-body search would happily accept an unrelated mention of the bound
    far above it. This reconstructs exactly the one command.
    """

    assert needle in body, f"{LADDER_PATH}: {CASE_NAME} never invokes {needle!r}"
    lines = body.split("\n")
    hit = next(index for index, line in enumerate(lines) if needle in line)
    first = hit
    while first > 0 and lines[first - 1].rstrip().endswith("\\"):
        first -= 1
    last = hit
    while last + 1 < len(lines) and lines[last].rstrip().endswith("\\"):
        last += 1
    return "\n".join(lines[first : last + 1])


def test_skill_rung_invokes_the_live_approval_gate_case() -> None:
    """An uninvoked case is dead code that can never go red."""

    rung = _function_body(LADDER_PATH.read_text(), "rung_skill", "# Rung 2: the compose tier")
    assert CASE_NAME in rung, (
        "the skill rung must call the live approval-gate case; a case defined "
        "but never invoked proves nothing and fails no run"
    )


def test_case_returns_early_before_booting_when_not_live() -> None:
    """A sealed run of this case would be a false green, not a cheap one."""

    body = _case_body()
    assert "LIVE" in body, (
        f"{CASE_NAME} must guard on $LIVE; the fake tier cannot exhibit either "
        "failure mode from #1852/#2068, so a sealed run reports a green that "
        "means nothing"
    )
    assert "skill up" in body, f"{CASE_NAME} must boot its own runner with `skill up`"
    assert body.index("LIVE") < body.index("skill up"), (
        "the $LIVE guard must come before the runner is booted, or the case "
        "runs sealed against the fake model and passes for the wrong reason"
    )


def test_case_arms_the_gate_with_a_name_only_secret() -> None:
    """`--secret NAME=VALUE` is silently dropped, and the gate never arms."""

    body = _case_body()
    assert "--secret CURIE_APPROVAL_REQUIRED_TOOLS" in body, (
        f"{CASE_NAME} must arm the operator gate via "
        "`--secret CURIE_APPROVAL_REQUIRED_TOOLS`"
    )

    # cli/src/docker.rs forwards a variable literally named as passed, so
    # `--secret CURIE_APPROVAL_REQUIRED_TOOLS=Bash` forwards nothing, the gate
    # never arms, and the turn ends `done` with a misleading message.
    #
    # This checks the ARGUMENT of each `--secret`, not the body text, because
    # the case legitimately contains `export CURIE_APPROVAL_REQUIRED_TOOLS=Bash`
    # -- a bare `"CURIE_APPROVAL_REQUIRED_TOOLS=" not in body` would make the
    # pin unsatisfiable. Walking the good string
    # `--secret CURIE_APPROVAL_REQUIRED_TOOLS` yields the next token
    # `CURIE_APPROVAL_REQUIRED_TOOLS`, which has no `=`; the bad string
    # `--secret CURIE_APPROVAL_REQUIRED_TOOLS=Bash` yields
    # `CURIE_APPROVAL_REQUIRED_TOOLS=Bash`, which does -- so this catches it.
    # The `export` line is never a `--secret` argument and is untouched.
    tokens = body.split()
    offenders: list[str] = []
    for position, token in enumerate(tokens):
        if token.startswith("--secret="):
            value = token[len("--secret=") :]
        elif token == "--secret":
            following = [candidate for candidate in tokens[position + 1 :] if candidate != "\\"]
            value = following[0] if following else ""
        else:
            continue
        if "=" in value:
            offenders.append(value)
    assert not offenders, (
        "`--secret` takes a bare variable NAME; a NAME=VALUE form is filtered "
        f"out of the container environment without a word, got: {offenders}"
    )


def test_case_bounds_the_turn_with_timeout() -> None:
    """`curie skill message` has no timeout, so an unbounded turn hangs forever.

    The bound is gnu-process.py's timeout rather than GNU's own, because a
    stock Mac ships no `timeout`, and the case must still run there.
    """

    body = _case_body()
    invocation = _logical_line_at(body, "skill message")
    assert '"$GNU_PROCESS" timeout "$GATE_CASE_TURN_SECONDS" ' in invocation, (
        "the `skill message` turn must run under gnu-process.py's `timeout`; "
        "nothing on that path bounds itself, so a #2068 revert wedges the whole "
        f"ladder instead of failing it. Unbounded invocation: {invocation!r}"
    )


def test_case_asserts_the_parked_status_and_the_unrun_canary() -> None:
    """Parked-and-unrun is the claim; a finalized reply is its inverse.

    Both halves are pinned by STRUCTURE, not by the presence of a string.
    `awaiting-approval` and the canary path both appear in this case's
    diagnostics, so a saboteur who replaces the real status comparison with an
    unconditional success, or the canary conditional with `false`, leaves every
    such string in place. What that saboteur must destroy is the comparison
    itself -- so that is what these assertions read.
    """

    body = _case_body()

    # --- (b) parked -------------------------------------------------------
    # Neutering shape 1: `if [[ "$status" == "awaiting-approval" ]]` becomes
    # `if true` / `if :`. That deletes this expression while the message a few
    # lines down still says "awaiting-approval".
    assert re.search(r'\[\[\s*"\$\{?status\}?"\s*==\s*"awaiting-approval"\s*\]\]', body), (
        "the case must compare the turn's parsed terminal status against "
        "`awaiting-approval` in a real test expression; the literal appearing "
        "only in a diagnostic proves nothing about what the case enforces"
    )

    # Neutering shape 2: keep the comparison, but hardcode what it reads. The
    # parser must consume the captured turn output and derive both compared
    # variables from that parsed result.
    parsed_assignments = re.findall(r'^\s*parsed=(.*)$', body, flags=re.MULTILINE)
    assert parsed_assignments, f"{CASE_NAME} must parse the captured turn output"
    assert any('$(' in value and '"$out"' in value for value in parsed_assignments), (
        "the parked-turn checks must be parsed out of the captured `skill message` "
        f"payload, not synthesized; got assignments: {parsed_assignments}"
    )

    status_assignments = re.findall(r'^\s*status=(.*)$', body, flags=re.MULTILINE)
    assert status_assignments, f"{CASE_NAME} must capture the turn's status"
    assert any("parsed" in value for value in status_assignments), (
        "the compared status must be derived from the parsed payload; got "
        f"assignments: {status_assignments}"
    )
    assert not any("awaiting-approval" in value for value in status_assignments), (
        "the expected status must never be assigned to the variable the case "
        f"then compares against it; got assignments: {status_assignments}"
    )

    parked_shape_assignments = re.findall(
        r'^\s*parked_shape=(.*)$', body, flags=re.MULTILINE
    )
    assert parked_shape_assignments, f"{CASE_NAME} must capture the parked JSON shape"
    assert any("parsed" in value for value in parked_shape_assignments), (
        "the compared parked shape must be derived from the parsed payload; got "
        f"assignments: {parked_shape_assignments}"
    )
    assert not any("valid" in value for value in parked_shape_assignments), (
        "the expected parked shape must never be assigned to the variable the "
        f"case compares; got assignments: {parked_shape_assignments}"
    )
    assert re.search(
        r'parked_shape\s*=\s*\(\s*"valid"\s*if\s*'
        r'payload\.get\("finalized"\)\s+is\s+False\s+and\s*'
        r'isinstance\(payload\.get\("approval_summary"\),\s*str\)\s*'
        r'else\s*"invalid"\s*\)',
        body,
    ), "parked_shape must be computed from finalized=false and approval_summary"
    assert re.search(
        r'\[\[\s*"\$\{?status\}?"\s*==\s*"awaiting-approval"\s*'
        r'&&\s*"\$\{?parked_shape\}?"\s*==\s*"valid"\s*\]\]',
        body,
    ), "the success branch must require both the parked status and JSON shape"

    # --- (c) unrun --------------------------------------------------------
    # Neutering shape 3: drop the in-container probe, or stop asking it a
    # definite question. The probe must run inside the container, name the
    # canary path, and print one of two unambiguous tokens.
    # The needle keeps the opening quote of the container name so it matches
    # the invocation and not the prose comment above it, which names the
    # rejected `docker exec ... test -e` shape.
    probe = _logical_line_at(body, 'docker exec "')
    for fragment in (CANARY_PATH, CANARY_PRESENT, CANARY_ABSENT):
        assert fragment in probe, (
            "the side-effect probe must run in the container and print a "
            f"definite token; {fragment!r} missing from: {probe!r}"
        )

    # Neutering shape 4: `if <condition>` becomes `if false`, so the
    # ran-anyway branch can never fire. Both tokens must be compared in a real
    # test expression, and each comparison must lead to a failure.
    for token in (CANARY_PRESENT, CANARY_ABSENT):
        assert re.search(rf'\[\[\s*"\$\{{?\w+\}}?"\s*[=!]=\s*"{token}"\s*\]\]', body), (
            f"the case must compare the probe's output against {token!r} in a "
            "real test expression; a conditional that cannot be false asserts "
            "nothing"
        )

    # Neutering shape 5: the original defect -- treat a probe that could not
    # run as proof the file is absent. The probe's exit status must be
    # captured AND inspected, so "docker could not answer" is its own failure
    # rather than a pass.
    code_vars = re.findall(r'(\w+)=\$\?', probe)
    assert code_vars, (
        "the probe must capture its own exit status: `docker exec` exits "
        "non-zero both when the canary is absent and when the command could "
        f"not run at all. Probe: {probe!r}"
    )
    assert any(re.search(rf'\(\(\s*{name}\s*[=!]=\s*0\s*\)\)', body) for name in code_vars), (
        "the probe's captured exit status must be inspected; capturing it and "
        "ignoring it lets a daemon or container failure read as `the gate held`"
    )

    # Both non-pass outcomes must end the case red.
    lines = body.split("\n")
    for index, line in enumerate(lines):
        if CANARY_PRESENT not in line and CANARY_ABSENT not in line:
            continue
        if "[[" not in line:
            continue
        assert any("return 1" in follower for follower in lines[index : index + 5]), (
            "every canary outcome other than a clean, definite absent token "
            f"must fail the case; this branch does not: {line.strip()!r}"
        )

    assert "--fake-model" not in body, (
        "the case proves real SDK dispatch against a real model; a "
        "`--fake-model` boot cannot exhibit either failure mode"
    )
    assert "assert_finalized_reply" not in body, (
        "assert_finalized_reply treats `awaiting_approval` as a FAILURE, which "
        "is the exact inverse of this case's claim; do not reuse it here"
    )


def test_cleanup_trap_reaps_the_gate_case_container() -> None:
    """The case boots a container; a run that dies mid-case must not strand it."""

    trap = _function_body(LADDER_PATH.read_text(), "cleanup", "trap cleanup EXIT")
    assert "GATE_CASE_CREATED" in trap, (
        "the global EXIT trap must reap the gate case's runner by its exact "
        "unique name, even when the case never reaches its own teardown"
    )


def test_case_uses_its_own_bundle_copy() -> None:
    """`skill up` writes state into its CWD bundle; the parity artifact is shared."""

    body = _case_body()
    assert CASE_BUNDLE in body, (
        f"the case must operate in its own {CASE_BUNDLE} copy: `skill up` "
        "writes .curie/ state into the CWD bundle, and $WORKDIR/bundle is the "
        "artifact assert_bundle_identity compares across rungs (#1608)"
    )

    # Mentioning $WORKDIR/bundle is not itself the defect -- `cp -a
    # "$WORKDIR/bundle" "$GATE_CASE_BUNDLE"` names it as the SOURCE and is
    # exactly what the case is supposed to do. The defect is the case
    # *operating in* it. So: every mention must be a `cp` source (a `cp` line,
    # and not that line's final argument, which is the destination), and no
    # `cd` may target it.
    for line in body.split("\n"):
        if PARITY_BUNDLE not in line:
            continue
        for chunk in line.replace("&&", ";").split(";"):
            if PARITY_BUNDLE not in chunk:
                continue
            tokens = chunk.split()
            assert tokens[0] == "cp", (
                f"{PARITY_BUNDLE} may only appear as the source of the bundle "
                f"copy, never as a path the case works in; got: {chunk.strip()!r}"
            )
            mentions = [index for index, token in enumerate(tokens) if PARITY_BUNDLE in token]
            assert max(mentions) < len(tokens) - 1, (
                f"{PARITY_BUNDLE} is the destination of this copy, which would "
                f"overwrite the shared parity artifact: {chunk.strip()!r}"
            )

    targets: list[str] = []
    for raw in body.split("\n"):
        # A `cd` may open a subshell (`(cd "$X" && ...)`) or a command
        # substitution (`out="$(cd "$X" && ...)"`), so strip the leading
        # punctuation rather than requiring the line to start with `cd`.
        statement = raw.strip().lstrip("(").lstrip()
        statement = statement.split('="$(', 1)[-1].lstrip()
        if statement.startswith("cd "):
            targets.append(statement.split()[1])
    assert targets, f"{CASE_NAME} must cd into the bundle copy before `skill up`"
    for target in targets:
        assert PARITY_BUNDLE not in target, (
            "the case must never cd into the shared parity bundle; "
            f"got `cd {target}`"
        )
