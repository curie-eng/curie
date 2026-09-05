"""Retry eligibility gate over the real GitHub Actions workflows (issue #1106).

The rule this file encodes: a workflow step may carry a retry only when it
ACQUIRES a third party dependency over the network before any repository code
has run. A step that builds, tests, or gates this repository's code must never
carry a retry, because a retry there hides a real defect instead of a transient
network fault.

The gate parses `.github/workflows/*.yaml` and `*.yml` directly. There are no
fixtures and no mocks: the whole value of this gate is that it reads what CI
actually runs.
"""

from __future__ import annotations

import functools
import re
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Any, NamedTuple

import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"

# Actions that only fetch a tool, an image, an artifact, or a registry session.
# Nothing here executes this repository's code, so a second attempt can only
# recover a network fault. Adding an entry is a deliberate eligibility decision.
ACQUISITION_ACTIONS = frozenset(
    {
        "astral-sh/setup-uv",
        "docker/setup-buildx-action",
        "docker/login-action",
        "actions/download-artifact",
        "helm/kind-action",
    }
)

# (workflow filename, job id, base step name). The base name is the attempt 1
# name: the backoff step and the `(retry)` copy of a wrap both fold onto it, so
# one entry covers all three steps of a single wrap.
RETRY_ALLOWLIST = frozenset(
    {
        ("ci.yaml", "python", "Install uv"),
        ("ci.yaml", "e2e-ladder-cluster", "Create the kind cluster"),
        ("dependency-audit.yaml", "python-audit", "Install uv"),
        ("gitleaks.yaml", "gitleaks", "Pull the gitleaks image"),
        ("release.yaml", "build", "Set up Buildx"),
        ("release.yaml", "build", "Log in to GHCR"),
        ("release.yaml", "merge", "Download digests"),
        ("release.yaml", "merge", "Set up Buildx"),
        ("release.yaml", "merge", "Log in to GHCR"),
        ("release.yaml", "worker-local-build", "Set up Buildx"),
        ("release.yaml", "worker-local-build", "Log in to GHCR"),
        ("release.yaml", "worker-local-merge", "Download digests"),
        ("release.yaml", "worker-local-merge", "Set up Buildx"),
        ("release.yaml", "worker-local-merge", "Log in to GHCR"),
        ("release.yaml", "release", "Download built binaries"),
    }
)

# A named canary on top of the closed world of rule 1: these are steps that run
# or gate this repository's own code, so a retry on any of them is always wrong.
#
# Naming note: issue #1106 refers to the docs step as "Interface catalog docs
# (generate + drift + citation lint)". That step was renamed on main before this
# work started, so the real current name is used below. A reader of the issue
# should expect the mismatch; a hardcoded stale name would be a dead assertion.
#
# Every name below was read out of the workflow file itself, never guessed. Rule
# 4's existence half turns a stale name into a red test rather than a silently
# dead assertion.
#
# Why this list is not redundant with rules 1 to 3: those state a positive
# property of whatever happens to be in the workflow, so deleting `Run gitleaks`
# from gitleaks.yaml outright leaves all three green. Only rule 4 reddens,
# because this list is a PRESENCE guard: the gate has to notice a security step
# disappearing, not merely notice it acquiring a retry.
PROTECTED_STEPS = frozenset(
    {
        ("ci.yaml", "python", "Pytest"),
        ("ci.yaml", "python", "Docs gate (catalog drift + agent contract + citations)"),
        ("ci.yaml", "rust", "Test"),
        ("ci.yaml", "ui", "Lint"),
        ("ci.yaml", "ui", "Command manifest is current"),
        ("ci.yaml", "commit-messages", "Check the PR's commit messages"),
        # Security gates. Retrying any of these reruns a scan or a verification
        # against the very same tree or the very same asset set, so a second
        # attempt cannot recover anything and can only bury a true positive.
        # `Run gitleaks` is the sharpest case: issue #1106 originally read the
        # 2026-07-24 gitleaks failures as a pull blip, and the job logs show
        # `leaks found: 2` instead. A retry there would have hidden two real
        # leaks. The image pull sits in its own step precisely so the scan can
        # stay retry free while the acquisition is wrapped.
        ("gitleaks.yaml", "gitleaks", "Run gitleaks"),
        ("codeql.yaml", "analyze", "Perform CodeQL Analysis"),
        ("dependency-audit.yaml", "cargo-audit", "Audit cli crate"),
        ("dependency-audit.yaml", "cargo-audit", "Audit generated ACI protocol crate"),
        ("dependency-audit.yaml", "python-audit", "pip-audit"),
        ("dependency-audit.yaml", "js-audit", "pnpm audit"),
        # Release authorization and asset integrity gates. A retry here would
        # re-ask a question whose answer is a property of the tag, the commit,
        # or the published asset set, none of which a second attempt changes.
        (
            "release.yaml",
            "authorize-release",
            "Tag's commit must be reviewed, on an allowed branch, and fully checked",
        ),
        ("release.yaml", "release", "Gate the asset set and build the checksum manifest"),
        ("release.yaml", "release", "Refuse to re-release an already-published tag"),
        ("release.yaml", "verify-and-publish", "Every asset is checksummed and carries an SBOM"),
        (
            "release.yaml",
            "verify-and-publish",
            "The checksum manifest signature is this workflow's",
        ),
        (
            "release.yaml",
            "verify-and-publish",
            "Every asset has provenance from this workflow and commit",
        ),
        ("close-on-next.yaml", "reconcile", "Close-on-next self-test"),
        ("close-on-next.yaml", "reconcile", "Reconcile issues"),
    }
)

RETRY_SUFFIX = " (retry)"
BACKOFF_PREFIX = "Back off before retrying "
RUN_LOOP_SENTINEL = "of 2, retrying in"
OUTCOME_REFERENCE = re.compile(r"steps\.([A-Za-z_][A-Za-z0-9_-]*)\.outcome")
# The only comparison a retry guard may make. Comparing to anything else (most
# plausibly 'success') leaves the wrap syntactically intact while it never fires
# on the one condition it exists for: attempt 1 having failed.
OUTCOME_FAILURE_GUARD = re.compile(r"steps\.([A-Za-z_][A-Za-z0-9_-]*)\.outcome\s*==\s*'failure'")

# Shell retry constructs, used by rule 4 to police protected `run:` bodies
# WITHOUT going through `_carries_retry`. `_carries_retry` recognises only the
# markers this repository happens to use today, so a hand rolled loop worded
# differently slips past it. The check below looks at shell shape instead of
# wording, which is what makes rule 4 independent of that blind spot.
LOOP_KEYWORDS = ("for ", "while ", "until ")
SLEEP_CALL = re.compile(r"(?:^|[\s;&|(])sleep\s", re.MULTILINE)
COMMAND_SEPARATORS = re.compile(r"\|\||&&|;|\n")
LINE_CONTINUATION = re.compile(r"\\\n")
# Shell scaffolding and pure output, which repeat harmlessly and say nothing
# about whether a command was invoked twice.
NON_INVOCATION_HEADS = frozenset(
    {"echo", "printf", "set", "if", "elif", "for", "while", "until", "case", "exit", "cd", "export"}
)
SHELL_KEYWORDS = frozenset({"do", "done", "fi", "then", "else", "esac", "true", "false"})

# The steps below have the shape of a retry loop without being retries. They are
# readiness polls for external state, and they never re invoke repository code
# or a mutation whose failure would be a defect. A readiness poll is not a retry.
#
# This is the ONLY escape from the closed world rule 4 draws around in `run:`
# retry constructs. Every other step in every workflow must either be on
# RETRY_ALLOWLIST or carry no such construct at all. Adding an entry here is a
# deliberate decision with the same weight as adding one to RETRY_ALLOWLIST.
RUN_RETRY_EXEMPT: frozenset[tuple[str, str, str]] = frozenset(
    {
        ("ci.yaml", "python", "Wait for Langfuse to serve"),
        # The candidate API has already rolled out; this only waits for the
        # temporary local port-forward to expose its external health state.
        (
            "ci.yaml",
            "e2e-released-upgrade",
            "Require healthy API after the candidate upgrade",
        ),
        # The raw CDN is eventually consistent after publication. This poll
        # never reruns repository code or the Pages publication mutation.
        ("chart-index.yaml", "publish-index", "Verify the public Helm consumer path"),
        # Both are readiness polls for an external process starting up, not
        # retries. The first waits for the stub upstream container to begin
        # serving; the second waits for nginx to begin serving, and the two
        # cannot be collapsed into one because nginx resolves its `proxy_pass`
        # upstream at startup, so the UI must be started only once the upstream
        # is already reachable. Neither re invokes repository code, and neither
        # retries a mutation whose failure would be a defect: if either never
        # becomes ready the step fails the job rather than papering over it.
        ("ci.yaml", "ui-image-smoke", "Start the stub API upstream"),
        ("ci.yaml", "ui-image-smoke", "Start the UI container"),
    }
)

Identity = tuple[str, str, str]


class LocatedStep(NamedTuple):
    """A step together with the position that fixes its order within its job."""

    index: int
    step: dict[str, Any]


class Trio(NamedTuple):
    """The three steps of one retry wrap. Any member may be absent."""

    attempt: LocatedStep | None
    backoff: LocatedStep | None
    retry: LocatedStep | None


@functools.cache
def _load_workflows() -> dict[str, dict[str, Any]]:
    """Parse every workflow file. Also a free structural check on the YAML.

    Memoised: the argument list is empty and the input is immutable on disk for
    the length of a run, so the six rules would otherwise reparse all nine
    files once each. Every consumer treats the parsed documents as read only,
    and the one place that needs to alter a step (`_iter_steps`, folding a job
    level `continue-on-error`) builds a shallow copy rather than writing back.
    """
    # Both extensions: GitHub Actions honours `.yml` exactly as it honours
    # `.yaml`, so a `.yaml`-only glob would let a whole workflow file opt out of
    # every rule in this gate by being named `ci.yml`.
    paths = sorted([*WORKFLOWS_DIR.glob("*.yaml"), *WORKFLOWS_DIR.glob("*.yml")])
    assert paths, f"no workflow files found under {WORKFLOWS_DIR}"
    loaded: dict[str, dict[str, Any]] = {}
    for path in paths:
        document = yaml.safe_load(path.read_text(encoding="utf-8"))
        assert isinstance(document, dict), f"{path.name} did not parse as a YAML mapping"
        # Note: the `on:` key parses as the boolean True under the YAML 1.1
        # loader pyyaml uses. Nothing here reads it, so that is harmless.
        loaded[path.name] = document
    return loaded


def _iter_jobs() -> Iterator[tuple[str, str, dict[str, Any], list[Any]]]:
    """Yield (filename, job id, job, steps) for every job that declares steps."""
    for filename, document in _load_workflows().items():
        jobs = document.get("jobs") or {}
        for job_id, job in jobs.items():
            if not isinstance(job, dict):
                continue
            steps = job.get("steps")
            if not steps:
                # A job that delegates to a reusable workflow has `uses:` and no
                # `steps:` at all.
                continue
            yield filename, job_id, job, steps


def _iter_steps() -> Iterator[tuple[str, str, int, dict[str, Any]]]:
    """Yield (filename, job id, step index, step) for every step in every job.

    A job level `continue-on-error: true` swallows the failure of every step in
    that job, which is a retry marker applied wholesale. It is folded onto each
    step here (on a shallow copy, never on the parsed document) so that every
    rule downstream sees it without having to know about the job level key.
    """
    for filename, job_id, job, steps in _iter_jobs():
        job_swallows = _continue_on_error(job)
        for index, step in enumerate(steps):
            if isinstance(step, dict):
                yield (
                    filename,
                    job_id,
                    index,
                    ({**step, "continue-on-error": True} if job_swallows else step),
                )


def _step_name(step: dict[str, Any]) -> str | None:
    name = step.get("name")
    return name if isinstance(name, str) else None


def _base_name(name: str) -> str:
    """Fold a backoff step or a retry copy back onto its attempt 1 name."""
    if name.startswith(BACKOFF_PREFIX):
        return name[len(BACKOFF_PREFIX) :]
    if name.endswith(RETRY_SUFFIX):
        return name[: -len(RETRY_SUFFIX)]
    return name


def _continue_on_error(step: dict[str, Any]) -> bool:
    value = step.get("continue-on-error")
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        # An expression such as ${{ ... }} is treated as carrying the marker,
        # because the gate cannot evaluate it and the safe reading is that it
        # can be true at runtime.
        return value.strip().lower() not in {"", "false"}
    return False


def _carries_retry(step: dict[str, Any]) -> bool:
    if _continue_on_error(step):
        return True
    condition = step.get("if")
    if isinstance(condition, str) and OUTCOME_REFERENCE.search(condition):
        return True
    name = _step_name(step)
    if name is not None and (name.endswith(RETRY_SUFFIX) or name.startswith(BACKOFF_PREFIX)):
        return True
    body = step.get("run")
    if isinstance(body, str) and RUN_LOOP_SENTINEL in body:
        return True
    return False


def _run_retry_construct(step: dict[str, Any]) -> str | None:
    """Describe a shell retry construct in a step's `run:` body, or None.

    Deliberately independent of `_carries_retry` and of every hardcoded wording
    marker. Two shapes count, both structural:

    * a loop keyword together with a `sleep`, which is the whole anatomy of a
      hand rolled retry loop no matter how its log line is phrased;
    * the same command invoked twice, which is the `cmd || cmd` retry with no
      loop and no sleep at all.
    """
    body = step.get("run")
    if not isinstance(body, str):
        return None
    loop = next((keyword.strip() for keyword in LOOP_KEYWORDS if keyword in body), None)
    if loop is not None and SLEEP_CALL.search(body):
        return f"a `{loop}` loop containing a `sleep`"
    # A backslash newline is a line continuation, so the two lines are ONE
    # command. Splitting there would compare wrapped tails to each other and
    # call two different `docker compose up` lines a repeat because their
    # trailing service list matches. Joining first compares whole commands,
    # which is what "invoked twice" is supposed to mean.
    joined = LINE_CONTINUATION.sub(" ", body)
    invocations: list[str] = []
    for fragment in COMMAND_SEPARATORS.split(joined):
        text = " ".join(fragment.split())
        # Splitting on `||` cuts inside `$( ... || true)` too, leaving a tail
        # such as `true)"`. Stripping the closing punctuation lets that reduce
        # to the shell keyword it is. A real command ending in `)` keeps its
        # name and is still counted.
        if not text or text.startswith("#") or text.rstrip(")}\"'") in SHELL_KEYWORDS:
            continue
        if text.split(" ", 1)[0] in NON_INVOCATION_HEADS:
            continue
        invocations.append(text)
    # Counter preserves insertion order, so the reported command is the first
    # repeated one in the body exactly as a linear rescan would report it.
    repeated = next(
        (invocation for invocation, count in Counter(invocations).items() if count > 1), None
    )
    if repeated is not None:
        return f"the command `{repeated}` invoked more than once"
    return None


def _locate_trio(steps: list[Any], base: str) -> Trio:
    """Find the attempt, backoff, and retry steps of one wrap, named by role."""
    attempt: LocatedStep | None = None
    backoff: LocatedStep | None = None
    retry: LocatedStep | None = None
    for index, step in enumerate(steps):
        if not isinstance(step, dict):
            continue
        name = _step_name(step)
        if name == base:
            attempt = LocatedStep(index, step)
        elif name == BACKOFF_PREFIX + base:
            backoff = LocatedStep(index, step)
        elif name == base + RETRY_SUFFIX:
            retry = LocatedStep(index, step)
    return Trio(attempt, backoff, retry)


def _allowlisted_uses_trios() -> Iterator[tuple[Identity, Trio]]:
    """Yield (identity, trio) for every allowlisted wrap around a `uses:` step.

    The one allowlisted `run:` step (the gitleaks image pull) is a self contained
    shell loop rather than a three step wrap, so it has no trio to check and is
    skipped. Rule 2 is what keeps that entry honest.
    """
    for filename, job_id, _job, steps in _iter_jobs():
        for step in steps:
            if not isinstance(step, dict):
                continue
            name = _step_name(step)
            if name is None or name != _base_name(name):
                continue
            identity = (filename, job_id, name)
            if identity not in RETRY_ALLOWLIST or not isinstance(step.get("uses"), str):
                continue
            yield identity, _locate_trio(steps, name)


def _is_acquisition(step: dict[str, Any]) -> bool:
    uses = step.get("uses")
    if isinstance(uses, str):
        return uses.split("@", 1)[0] in ACQUISITION_ACTIONS
    body = step.get("run")
    if isinstance(body, str):
        return "docker pull" in body
    return False


def _steps_by_identity() -> dict[Identity, list[dict[str, Any]]]:
    index: dict[Identity, list[dict[str, Any]]] = {}
    for filename, job_id, _index, step in _iter_steps():
        name = _step_name(step)
        if name is None:
            continue
        index.setdefault((filename, job_id, _base_name(name)), []).append(step)
    return index


def _trio_problems(base: str, trio: Trio) -> list[str]:
    """List everything wrong with one wrap's anatomy. Rule 6 states the why.

    A plain function rather than a closure inside rule 6: the identity of the
    wrap is added by the caller, so nothing here has to capture a loop variable.
    """
    problems: list[str] = []
    attempt, backoff, retry = trio
    if attempt is None:
        return ["no attempt step"]
    if backoff is None:
        problems.append(f"no `{BACKOFF_PREFIX}{base}` step")
    if retry is None:
        problems.append(f"no `{base}{RETRY_SUFFIX}` step")
    if backoff is None or retry is None:
        return problems
    if not (attempt.index < backoff.index < retry.index):
        problems.append("attempt, backoff, and retry are out of order")
    if not _continue_on_error(attempt.step):
        problems.append("attempt carries no `continue-on-error: true`, so the retry is dead")
    if not isinstance(attempt.step.get("id"), str):
        problems.append("attempt declares no `id:`, so no guard can reference its outcome")
    if "continue-on-error" in retry.step:
        problems.append("retry copy carries `continue-on-error`, so its failure is swallowed too")
    if attempt.step.get("uses") != retry.step.get("uses"):
        problems.append(
            "retry copy's `uses:` differs from the attempt's: "
            f"{attempt.step.get('uses')!r} vs {retry.step.get('uses')!r}"
        )
    if attempt.step.get("with") != retry.step.get("with"):
        problems.append("retry copy's `with:` block differs from the attempt's")
    return problems


def _render(entries: list[Identity]) -> str:
    return "\n".join(f"  {filename} :: {job} :: {name}" for filename, job, name in sorted(entries))


def test_rule_1_every_retry_carrying_step_is_allowlisted() -> None:
    """Closed world: no step anywhere may carry a retry unless it is allowlisted.

    This is deliberately a sweep over every step of every workflow file, present
    and future, rather than a denylist of the steps we happen to know about. A
    workflow added tomorrow is covered without anyone editing a list.
    """
    unexpected: list[Identity] = []
    for filename, job_id, index, step in _iter_steps():
        if not _carries_retry(step):
            continue
        name = _step_name(step)
        if name is None:
            unexpected.append((filename, job_id, f"<unnamed step at index {index}>"))
            continue
        identity = (filename, job_id, _base_name(name))
        if identity not in RETRY_ALLOWLIST:
            unexpected.append(identity)
    assert not unexpected, (
        "these steps carry a retry marker but are not on RETRY_ALLOWLIST. A retry "
        "is permitted only on a step that acquires a third party dependency before "
        "any repository code runs. Never on a step that builds, tests, or gates "
        "this repository's code:\n" + _render(unexpected)
    )


def test_rule_2_every_allowlist_entry_resolves_to_a_retried_step() -> None:
    """No stale entry may sit on the allowlist granting permission to nothing."""
    index = _steps_by_identity()
    missing: list[Identity] = []
    unwrapped: list[Identity] = []
    for entry in RETRY_ALLOWLIST:
        steps = index.get(entry)
        if not steps:
            missing.append(entry)
        elif not any(_carries_retry(step) for step in steps):
            unwrapped.append(entry)
    assert not missing, (
        "these RETRY_ALLOWLIST entries name a step that does not exist. Either the "
        "wrap was never added, or the step was renamed and the allowlist rotted:\n"
        + _render(missing)
    )
    assert not unwrapped, (
        "these RETRY_ALLOWLIST entries name a real step that carries no retry "
        "marker, so a wrap was dropped in a later refactor:\n" + _render(unwrapped)
    )


def test_rule_3_every_allowlisted_step_is_an_acquisition_step() -> None:
    """The eligibility rule itself, encoded.

    An allowlisted attempt step must use one of ACQUISITION_ACTIONS, or be a
    `run:` step that pulls an image. Allowlisting a test or a gate therefore
    fails here even if rule 4's name list has gone stale.
    """
    ineligible: list[Identity] = []
    for filename, job_id, _index, step in _iter_steps():
        name = _step_name(step)
        if name is None or name.startswith(BACKOFF_PREFIX):
            # The backoff step is a sleep, not an attempt, so it is not held to
            # the acquisition rule.
            continue
        identity = (filename, job_id, _base_name(name))
        if identity in RETRY_ALLOWLIST and not _is_acquisition(step):
            ineligible.append((filename, job_id, name))
    assert not ineligible, (
        "these allowlisted steps do not acquire a third party dependency, so they "
        "are not eligible for a retry:\n" + _render(ineligible)
    )


def test_rule_4_no_protected_step_carries_a_retry() -> None:
    """The steps that run or gate our own code stay retry free, and stay present.

    The existence half is deliberate brittleness. Renaming one of these breaks
    this test, which forces a human to check that the protection followed the
    rename rather than silently evaporating.

    The shell half deliberately does NOT go through `_carries_retry`. That helper
    recognises only the markers this repository uses today, so a retry loop
    worded differently from its sentinel would sail past it. Rule 4 is the rule
    that matters most, so it checks the shape of the `run:` body directly and
    holds even if every marker in `_carries_retry` goes stale.

    That shell half then runs over EVERY step in every workflow, not only the
    protected ones. The other two retry mechanisms (a marker on the step, a
    three step wrap) are already closed worlds under rule 1, so leaving the in
    `run:` mechanism scoped to a name list was the one way left to add a retry
    that no rule here would see: put it in a `run:` body of a step nobody
    thought to protect. The sweep below closes that, with RETRY_ALLOWLIST and
    RUN_RETRY_EXEMPT as the only two ways out.
    """
    index = _steps_by_identity()
    missing: list[Identity] = []
    wrapped: list[Identity] = []
    looped: list[Identity] = []
    for entry in PROTECTED_STEPS:
        steps = index.get(entry)
        if not steps:
            missing.append(entry)
            continue
        if any(_carries_retry(step) for step in steps):
            wrapped.append(entry)
        for step in steps:
            construct = _run_retry_construct(step)
            if construct is not None:
                looped.append((entry[0], entry[1], f"{entry[2]} -> {construct}"))
    assert not missing, (
        "these PROTECTED_STEPS no longer exist. If the step was renamed, update "
        "PROTECTED_STEPS and confirm the new step still carries no retry:\n" + _render(missing)
    )
    assert not wrapped, (
        "these steps run or gate this repository's code and must never carry a "
        "retry, because a retry there hides a real defect:\n" + _render(wrapped)
    )
    assert not looped, (
        "these protected steps hand roll a retry inside their `run:` body. A "
        "shell loop is a retry even when it carries none of the markers this "
        "gate looks for, and it hides a real defect exactly the same way:\n" + _render(looped)
    )

    # The closed world half: the same helper over every step in every workflow.
    unaccounted: list[Identity] = []
    exempt_hits: set[Identity] = set()
    for filename, job_id, step_index, step in _iter_steps():
        construct = _run_retry_construct(step)
        if construct is None:
            continue
        name = _step_name(step)
        label = name if name is not None else f"<unnamed step at index {step_index}>"
        identity = (filename, job_id, _base_name(label))
        if identity in RUN_RETRY_EXEMPT:
            exempt_hits.add(identity)
            continue
        if identity in RETRY_ALLOWLIST:
            continue
        unaccounted.append((filename, job_id, f"{label} -> {construct}"))
    assert not unaccounted, (
        "these steps hand roll a retry inside their `run:` body without being on "
        "RETRY_ALLOWLIST or RUN_RETRY_EXEMPT. A `run:` body is the third way to "
        "retry and it is closed world like the other two: either the step is an "
        "eligible acquisition, or it is not retrying:\n" + _render(unaccounted)
    )
    # Positive exercise of `_run_retry_construct` against real workflow steps.
    # Both readiness polls use a loop keyword together with a sleep, so all this
    # guards is the LOOP_KEYWORDS and SLEEP_CALL half of the helper: a typo in
    # either would satisfy every rule above just as well as a clean repository.
    #
    # It says nothing about COMMAND_SEPARATORS or NON_INVOCATION_HEADS. The
    # helper returns from the loop branch before the duplicate invocation
    # splitter ever runs, so these exemptions never exercise that branch at all.
    # The duplicate invocation branch is guarded instead by
    # test_run_retry_construct_detects_every_constructed_shape below, which
    # calls the helper directly on constructed `cmd || cmd` bodies and asserts
    # both directions.
    assert exempt_hits == RUN_RETRY_EXEMPT, (
        "`_run_retry_construct` no longer detects the retry construct in these "
        "RUN_RETRY_EXEMPT steps. Either the step changed and the exemption is now "
        "dead weight, or the helper has gone blind and every other assertion in "
        "this rule is passing vacuously:\n" + _render(sorted(RUN_RETRY_EXEMPT - exempt_hits))
    )


def test_rule_5_every_step_outcome_reference_resolves_and_tests_failure() -> None:
    """A `steps.X.outcome` guard must resolve in job, and must compare to 'failure'.

    Resolution is the copy paste error the retry mechanism invites most:
    duplicating a wrap into another job and forgetting to change the id. The
    reference then evaluates to empty, the retry silently never runs, and nothing
    notices until that workflow next fails for real.

    Comparison is the same defect wearing a different hat. A guard reading
    `steps.install_uv.outcome == 'success'` resolves perfectly, satisfies every
    other rule in this file, and still never retries on the one condition that
    matters. Resolving is only half of being correct.
    """
    dangling: list[Identity] = []
    miscompared: list[Identity] = []
    resolved: set[Identity] = set()
    for filename, job_id, _job, steps in _iter_jobs():
        declared_ids: set[str] = set()
        for index, step in enumerate(steps):
            if not isinstance(step, dict):
                continue
            condition = step.get("if")
            if isinstance(condition, str):
                label = _step_name(step) or f"<unnamed step at index {index}>"
                for match in OUTCOME_REFERENCE.finditer(condition):
                    referenced = match.group(1)
                    if referenced not in declared_ids:
                        dangling.append((filename, job_id, f"{label} -> {referenced}"))
                    else:
                        resolved.add((filename, job_id, referenced))
                    if OUTCOME_FAILURE_GUARD.match(condition, match.start()) is None:
                        miscompared.append(
                            (filename, job_id, f"{label} -> {' '.join(condition.split())}")
                        )
            step_id = step.get("id")
            if isinstance(step_id, str):
                declared_ids.add(step_id)
    assert not dangling, (
        "these `if:` guards reference a step outcome that no earlier step in the "
        "same job declares an id for:\n" + _render(dangling)
    )
    assert not miscompared, (
        "these `if:` guards reference a step outcome but do not compare it to "
        "'failure'. A retry must fire when, and only when, attempt 1 failed:\n"
        + _render(miscompared)
    )
    # Vacuity guard, keyed per allowlisted wrap rather than to a bare count. A
    # global count is satisfied by any one outcome reference anywhere in the
    # repository, so it stays green while thirteen of the fourteen guards are
    # deleted. This demands that every wrap's own attempt id is actually
    # referenced by a resolving guard in its own job.
    unguarded: list[Identity] = []
    for (filename, job_id, base), trio in _allowlisted_uses_trios():
        attempt = trio.attempt
        attempt_id = attempt.step.get("id") if attempt is not None else None
        if not isinstance(attempt_id, str) or (filename, job_id, attempt_id) not in resolved:
            unguarded.append((filename, job_id, f"{base} -> {attempt_id!r}"))
    assert not unguarded, (
        "these allowlisted wraps have no resolving `steps.<id>.outcome` guard on "
        "their own attempt id, so the wrap is inert and this rule would be "
        "asserting nothing about them:\n" + _render(unguarded)
    )


def test_rule_6_every_allowlisted_wrap_is_a_complete_faithful_trio() -> None:
    """The retry mechanism's own anatomy, asserted.

    Rules 1 to 5 police WHERE a retry may sit. None of them police whether the
    thing sitting there is a working retry, and three mutations exploit that:

    * Delete the backoff and `(retry)` steps but keep `continue-on-error: true`
      on the attempt. Every other rule stays green, and the result is strictly
      worse than making no change at all: the step's failure is now silently
      swallowed and the job carries on without the dependency it needed.
    * Drop `continue-on-error` from the attempt, or add it to the `(retry)`
      copy. The first makes the wrap dead (attempt 1 fails the job before the
      retry can run); the second makes a failed retry vanish too.
    * Change the `(retry)` copy's `uses:` ref or its `with:` block. The copy is
      duplicated eleven times in release.yaml, four of those blocks carrying
      `${{ secrets.GITHUB_TOKEN }}`, and a swapped action SHA in one of them is
      dormant on the happy path: nothing runs the retry until attempt 1 fails,
      so nothing surfaces the drift until the day it matters.

    So: the trio must exist, in order, in the same job; the attempt must carry
    both `continue-on-error: true` and an `id:`; the retry copy must be an exact
    `uses:` and `with:` twin of the attempt; and it must carry no
    `continue-on-error` of its own.
    """
    broken: list[Identity] = []
    checked = 0
    for (filename, job_id, base), trio in _allowlisted_uses_trios():
        checked += 1
        broken.extend(
            (filename, job_id, f"{base} -> {problem}") for problem in _trio_problems(base, trio)
        )
    assert not broken, (
        "these allowlisted retry wraps are not a complete, faithful trio. A wrap "
        "that is incomplete is worse than no wrap at all, because the attempt's "
        "failure is swallowed and the job proceeds without its dependency:\n" + _render(broken)
    )
    # Vacuity guard: every allowlist entry bar the gitleaks image pull is a
    # `uses:` wrap, so this rule must have inspected all of them.
    expected = len(RETRY_ALLOWLIST) - 1
    assert checked == expected, (
        f"rule 6 inspected {checked} allowlisted `uses:` wraps but RETRY_ALLOWLIST "
        f"implies {expected}. Either a wrap's attempt step vanished or it stopped "
        "being a `uses:` step, and this rule silently stopped checking it"
    )


def test_run_retry_construct_detects_every_constructed_shape() -> None:
    """Direct unit test over `_run_retry_construct`, independent of any workflow file.

    Rule 4's closed world sweep only ever exercises the helper positively
    through RUN_RETRY_EXEMPT, whose readiness polls use a loop keyword together
    with a sleep. They return from the loop branch before the duplicate
    invocation splitter ever runs, so the branch that catches a hand rolled
    `cmd || cmd` retry with no loop and no sleep has zero positive coverage
    anywhere in this file: setting COMMAND_SEPARATORS so it never matches
    deletes `cmd || cmd` detection outright, and every rule above still passes.

    This test builds `run:` bodies directly and calls the helper on each,
    asserting both directions: constructed retries must be found, and
    constructed non-retries must not be. Confirmed by hand while writing this
    test that blinding COMMAND_SEPARATORS (so it never matches) or blinding
    NON_INVOCATION_HEADS (so it excludes everything, treating every fragment as
    scaffolding) reds this test.
    """
    detected: dict[str, str] = {
        "a for loop with a sleep": "for i in 1 2 3; do\n  sleep 1\ndone",
        "a while loop with a sleep": "while true; do\n  sleep 2\ndone",
        "an until loop with a sleep": ("until curl -sf http://target; do\n  sleep 2\ndone"),
        "the same command invoked twice on one line with ||": (
            "run-lint --strict || run-lint --strict"
        ),
        "the same command duplicated across a backslash continuation": (
            "run-migration \\\n  --target prod \\\n  --confirm\n"
            "run-migration \\\n  --target prod \\\n  --confirm"
        ),
        "a backslash continued command duplicated via ||": (
            "probe-service \\\n  --retries 3 || probe-service \\\n  --retries 3"
        ),
    }
    not_detected: dict[str, str] = {
        "a single plain command": "run-build --release",
        "two genuinely different commands in sequence": ("run-build --release\nrun-tests --unit"),
        "a sleep with no loop": "sleep 5",
        "a loop with no sleep": "for i in 1 2 3; do\n  echo $i\ndone",
        # The real false positive this guards against: two `docker compose up`
        # invocations that are genuinely different (one bare, one with --wait
        # flags and a shorter service list) but that both end in the identical
        # continuation line "langfuse-web langfuse-worker otel-collector".
        # Splitting on a raw backslash-newline before joining continuations
        # would compare that shared tail line to itself and call it a repeat.
        # Joining continuations first, as the helper does, compares whole
        # commands instead, and these two are not the same command.
        "the Start dev stack shape: similar commands over backslash continuations": (
            "docker compose -f compose.dev.yaml up -d \\\n"
            "  postgres valkey clickhouse rustfs rustfs-init \\\n"
            "  langfuse-web langfuse-worker otel-collector\n"
            "docker compose -f compose.dev.yaml up -d --wait --wait-timeout 300 \\\n"
            "  postgres valkey clickhouse rustfs \\\n"
            "  langfuse-web langfuse-worker otel-collector"
        ),
        # Splitting on `||` cuts inside `$(... || true)` too, leaving a tail
        # such as "true)". Two of these in one body would both reduce to the
        # bare word "true" and look like the same command invoked twice unless
        # that tail is recognised as shell scaffolding rather than a command.
        "the $(... || true) shape": (
            "first=$(cmd-one 2>/dev/null || true)\nsecond=$(cmd-two 2>/dev/null || true)"
        ),
    }
    missed = [
        label for label, body in detected.items() if _run_retry_construct({"run": body}) is None
    ]
    assert not missed, (
        "`_run_retry_construct` failed to detect a retry construct in these "
        "deliberately constructed bodies, so the branch that should have caught "
        "them is gone or blinded:\n" + "\n".join(f"  {label}" for label in missed)
    )
    false_positives = [
        f"{label} -> {result!r}"
        for label, body in not_detected.items()
        if (result := _run_retry_construct({"run": body})) is not None
    ]
    assert not false_positives, (
        "`_run_retry_construct` reported a retry construct in these bodies that "
        "are not one, which reopens a false positive this helper was written to "
        "avoid:\n" + "\n".join(f"  {entry}" for entry in false_positives)
    )
    # Confirm each detected case was caught by the branch it is meant to test,
    # not by the other branch coincidentally. A plain "is not None" check above
    # would not notice the loop branch and the duplicate invocation branch
    # trading places.
    for label in (
        "a for loop with a sleep",
        "a while loop with a sleep",
        "an until loop with a sleep",
    ):
        result = _run_retry_construct({"run": detected[label]})
        assert result is not None and "loop" in result and "sleep" in result, (
            f"{label} was detected, but not by the loop branch: {result!r}"
        )
    for label in (
        "the same command invoked twice on one line with ||",
        "the same command duplicated across a backslash continuation",
        "a backslash continued command duplicated via ||",
    ):
        result = _run_retry_construct({"run": detected[label]})
        assert result is not None and "invoked more than once" in result, (
            f"{label} was detected, but not by the duplicate invocation branch: {result!r}"
        )
