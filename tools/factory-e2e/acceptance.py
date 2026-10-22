"""The factory acceptance table: one entry per tracker and code host pairing.

`curie dev factory-e2e acceptance --pairing <tracker>+<code host>[@<target>]`
runs one entry; a forge with several deployments keys one entry per target.
Each entry fixes its steps, and each step fixes its inputs, the
`factory_e2e.py` command it runs, a deadline, what the forge must show
(remote assertions, observed by the scenario and recorded in its evidence),
the evidence fields that must hold (persisted assertions, as dotted path and
expected value), and its teardown checks.

A real run executes each step's command as a subprocess with the shared flags
and a per-step `--evidence` file, under the step's deadline. When the deadline
passes the subprocess gets SIGTERM, so it still tears down, and is killed
only if teardown overruns its grace. Then the step's exit code, every
persisted assertion and every teardown check are judged, the namespace check
reading the kube context with `kubectl --context`. One summary goes to stdout
and to `--evidence` when given.

`--dry-run` lists every step and launches nothing. It fails on an entry with
an unset step field, and reports "not installed" for an entry whose forge
drivers are not installed. github+github,
jira_cloud+github, jira_cloud+bitbucket_cloud, jira_cloud+bitbucket_dc,
gitlab+gitlab@fixture and gitlab+gitlab@saas are installed today;
jira_cloud+gitlab is a complete step definition waiting on its install path.
Bitbucket pairs only with a Jira Cloud tracker, installed by the combined
`curie factory quickstart --tracker jira-cloud --code-host bitbucket-*`.

The jira_cloud+github entry is driven by the jira_cloud driver (`--forge
jira_cloud`), which delegates the change, CI, review and merge steps to the
GitHub driver, so it asserts the same loop plus the tracker evidence. A
Bitbucket entry is driven by its code host's driver, which delegates the
ticket to the jira_cloud driver and asserts the loop only. Every Jira entry
asserts the "close Jira test issues" teardown. jira_cloud+bitbucket_dc brings
a Bitbucket Data Center fixture up for the run unless
CURIE_BITBUCKET_DC_FIXTURE names one, and removes the one it started.
jira_cloud+bitbucket_cloud reads its test repository, the two token files and
the reviewer's account id from CURIE_BITBUCKET_CLOUD_* variables.

The GitLab entries are driven by the gitlab driver with `--forge-target`.
`@fixture` brings a GitLab CE fixture up for the run on the kube context
node's address and removes it, and its teardown check reads the fixture's
state directory as gone; it also asserts that a Reporter's label admits
nothing. `@saas` reads its gitlab.com test project and token files from the
environment, and its live run fails before any step when one is unset.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import os
import signal
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, TextIO

import factory_e2e as fe

SCHEMA = "curie.factory-e2e.acceptance/v1"
FACTORY_E2E = Path(__file__).resolve().with_name("factory_e2e.py")

# Building the candidate images, installing and proving admission.
INSTALL_ALLOWANCE_SECONDS = 3600
# What the harness may spend undoing everything after the loop.
TEARDOWN_ALLOWANCE_SECONDS = 900
# After SIGTERM at the deadline, how long teardown may run before a kill.
TEARDOWN_GRACE_SECONDS = TEARDOWN_ALLOWANCE_SECONDS
# One request's longest wait in the loop: it may sit unstarted up to the cap,
# then run to its execution deadline plus the publication allowance.
REQUEST_WAIT_SECONDS = (
    fe.NEVER_STARTED_CAP_SECONDS + fe.EXECUTION_BOUND_SECONDS + fe.PUBLICATION_ALLOWANCE_SECONDS
)
# The loop has two requests (the labelled run with its CI fix round, then the
# review revision), then waits for the merge to read back.
LOOP_DEADLINE_SECONDS = (
    INSTALL_ALLOWANCE_SECONDS
    + 2 * REQUEST_WAIT_SECONDS
    + fe.LOOP_MERGE_OBSERVE_SECONDS
    + TEARDOWN_ALLOWANCE_SECONDS
)

TEARDOWN_CLEAN = "every teardown step in the evidence is ok (teardown_clean is true)"
FIXTURE_GONE = (
    "the GitLab fixture is gone: the evidence names gitlab_fixture.state_dir and that "
    "directory no longer exists (forge-fixture gitlab down deletes it only once nothing "
    "labelled with its compose project remains)"
)
NAMESPACE_GONE = (
    "the run's namespace is gone: kubectl --context <context> get namespace <namespace> "
    "--ignore-not-found -o name exits 0 and prints nothing"
)
# The classified expiry labels `curie cluster status --json` reports per forge
# credential (cli/src/forge_expiry.rs `classify(...).label()`).
CREDENTIALS_EXPIRY_LABELS = ("known", "unknown", "none")
# The expected value of the `scenario.cluster_status.credentials` persisted
# assertion (#4866): a plain string, so the dry run prints it verbatim and the
# judge dispatches on it to check every recorded row's expiry against the
# labels instead of one expected value.
CREDENTIALS_EXPIRY_VALID = "every credential's expiry is one of known, unknown or none"

Runner = Callable[[list[str], float], int]
Kubectl = Callable[[list[str]], "subprocess.CompletedProcess[str]"]


@dataclasses.dataclass(frozen=True)
class Step:
    name: str
    inputs: Mapping[str, str]
    # factory_e2e.py argv; the shared flags and --evidence are appended.
    command: list[str]
    deadline_seconds: int | None
    remote_assertions: tuple[str, ...]
    # (dotted evidence path, expected value)
    persisted_assertions: tuple[tuple[str, Any], ...]
    teardown_checks: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class Pairing:
    tracker: str
    code_host: str
    installed: bool
    steps: tuple[Step, ...]
    # The forge deployment the entry drives (`--forge-target`); None for a forge with one.
    target: str | None = None

    @property
    def key(self) -> str:
        pair = f"{self.tracker}+{self.code_host}"
        return f"{pair}@{self.target}" if self.target else pair


_FORGE_LABELS = {
    "github": "GitHub",
    "gitlab": "GitLab",
    "jira_cloud": "Jira Cloud",
    "bitbucket_cloud": "Bitbucket Cloud",
    "bitbucket_dc": "Bitbucket Data Center",
}
# What each code host calls the change the factory opens.
_CHANGE_NAMES = {
    "github": "pull request",
    "gitlab": "merge request",
    "bitbucket_cloud": "pull request",
    "bitbucket_dc": "pull request",
}
_CI_NAMES = {
    "github": "the failing factory-e2e/loop-gate commit status",
    "gitlab": "a failing external commit status",
    "bitbucket_cloud": "a failing build status",
    "bitbucket_dc": "a failing build status",
}


# The step the jira_cloud driver's teardowns() pushes, also on a Bitbucket driver.
CLOSE_JIRA_STEP = "close Jira test issues"
# What a passing Jira tracker run records under scenario.tracker.
_JIRA_TRACKER_ASSERTIONS = (
    ("scenario.tracker.status_comments", 1),
    ("scenario.tracker.remote_links", 1),
    ("scenario.tracker.pr_body_has_key", True),
    ("scenario.tracker.bot_label_admitted", False),
)


def _gitlab() -> Any:
    """The gitlab forge entry module (forges/gitlab.py)."""

    fe.load_forge_entries()
    return sys.modules["factory_e2e_forges.gitlab"]


def _bitbucket_dc() -> Any:
    """The bitbucket_dc forge entry module (forges/bitbucket_dc.py)."""

    fe.load_forge_entries()
    return sys.modules["factory_e2e_forges.bitbucket_dc"]


def _driver(tracker: str, code_host: str) -> str:
    """The driver the loop runs, which --forge and the evidence forge name.

    A Jira tracker's own driver, which delegates the code host steps, except
    on Bitbucket, whose drivers delegate the ticket to the Jira driver; else
    the code host's.
    """

    if tracker == "jira_cloud" and not code_host.startswith("bitbucket"):
        return tracker
    return code_host


def _inputs(tracker: str, code_host: str) -> dict[str, str]:
    if code_host == "bitbucket_cloud":
        credentials = (
            "CURIE_FACTORY_* variables for the cluster, the layer registry and a real model "
            "(CURIE_FACTORY_MODEL_API_KEY); CURIE_BITBUCKET_CLOUD_REPO names the test "
            "repository, CURIE_BITBUCKET_CLOUD_TOKEN_FILE the factory's and "
            "CURIE_BITBUCKET_CLOUD_REVIEWER_TOKEN_FILE the reviewer's repository access "
            "token files, and CURIE_BITBUCKET_CLOUD_REVIEWER_ACCOUNT_ID the reviewer's account"
        )
    elif code_host == "bitbucket_dc":
        credentials = (
            "CURIE_FACTORY_* variables for the cluster, the layer registry and a real model "
            "(CURIE_FACTORY_MODEL_API_KEY); the Bitbucket Data Center fixture is brought up "
            "for the run with `forge-fixture bitbucket-dc up` on the InternalIP of the kube "
            "context node running the Docker daemon, unless CURIE_BITBUCKET_DC_FIXTURE names "
            "a running one, and removed after it"
        )
    else:
        credentials = (
            "CURIE_FACTORY_* variables and files for the test App, actor and fixture, "
            "with CURIE_FACTORY_MODEL_API_KEY for a real model"
        )
    if tracker == "jira_cloud":
        credentials += (
            ", and CURIE_FACTORY_JIRA_ENV_FILE naming the Jira test site, the labeler and "
            "the factory account with their token files"
        )
    return {
        "tracker": f"{_FORGE_LABELS[tracker]} ({tracker})",
        "code_host": f"{_FORGE_LABELS[code_host]} ({code_host})",
        "ticket": "--issue-file, else the default loop ticket (nautical miles in unitconv)",
        "review": "--revision-file, else the default review text (a round trip unit test)",
        "candidate": "--candidate and --candidate-images (build for a pull request head)",
        "kube_context": "--context, else CURIE_FACTORY_KUBE_CONTEXT",
        "credentials": credentials,
    }


def _remote(tracker: str, code_host: str) -> tuple[str, ...]:
    change = _CHANGE_NAMES[code_host]
    host = _FORGE_LABELS[code_host]
    tracker_lines: tuple[str, ...] = ()
    if _driver(tracker, code_host) == "jira_cloud":
        tracker_lines = (
            "the ticket carries exactly one status comment from the factory account",
            f"the ticket carries exactly one remote link to the {change}",
            f"the {change} body names the ticket key and its browse URL",
            "a second ticket labelled by the factory account admits no WorkItem",
        )
    return (
        f"{_FORGE_LABELS[tracker]} shows one new ticket from the test actor carrying the "
        "factory label",
        f"{host} shows exactly one {change} opened by the factory for that ticket",
        f"the {change}'s first head carries {_CI_NAMES[code_host]} (CI red)",
        "a later head pushed by the factory has passing checks (CI green)",
        f"a review from the test actor that mentions the factory is followed by a new "
        f"commit on the same {change}",
        f"the test actor merged the {change} and {host} reads it merged",
        *tracker_lines,
    )


def _persisted(
    scenario: str, forge: str, pairing: str, teardown_steps: tuple[str, ...]
) -> tuple[Any, ...]:
    tracker = _JIRA_TRACKER_ASSERTIONS if forge == "jira_cloud" else ()
    return (
        ("result", "passed"),
        ("mode", f"run:{scenario}"),
        ("forge", forge),
        ("scenario.verdict", "passed"),
        ("scenario.forge", forge),
        ("scenario.pairing", pairing),
        ("scenario.red_ci.state", "failure"),
        ("scenario.ci_after_fix.state", "passing"),
        ("scenario.review_delivery_api_status", "factory_admitted"),
        ("scenario.merge.merged", True),
        ("scenario.forge_merged", True),
        ("scenario.work_item_pr_status", "merged"),
        ("scenario.cli_pr_status", "merged"),
        # The reads the loop records before teardown (#4866): the per-run
        # cost is complete, and every recorded credential row carries one of
        # the classified expiry labels.
        ("scenario.usage.cost_complete", True),
        ("scenario.cluster_status.credentials", CREDENTIALS_EXPIRY_VALID),
        *tracker,
        ("teardown_clean", True),
        *((f"teardown_ok.{name}", True) for name in teardown_steps),
    )


def _loop_step(tracker: str, code_host: str) -> Step:
    driver = _driver(tracker, code_host)
    if (tracker, code_host) == ("github", "github"):
        scenario = "github-loop"
        command = ["run", "--scenario", scenario]
        # The GitHub App's webhook is repointed at the install and put back.
        teardown_steps: tuple[str, ...] = (
            "restore App webhook",
            "reset fixture repository",
            "restore fixture default branch",
            "delete namespaces",
        )
    else:
        scenario = "forge-loop"
        command = ["run", "--scenario", scenario, "--forge", driver]
        teardown_steps = (
            *((CLOSE_JIRA_STEP,) if tracker == "jira_cloud" else ()),
            "reset fixture repository",
            "restore fixture default branch",
            "delete namespaces",
            # The fixture the run brought up is removed last.
            *((_bitbucket_dc().FIXTURE_DOWN_STEP,) if code_host == "bitbucket_dc" else ()),
        )
    return Step(
        name=f"{scenario} {tracker}+{code_host}",
        inputs=_inputs(tracker, code_host),
        command=command,
        deadline_seconds=LOOP_DEADLINE_SECONDS,
        remote_assertions=_remote(tracker, code_host),
        persisted_assertions=_persisted(scenario, driver, f"{tracker}+{code_host}", teardown_steps),
        teardown_checks=(TEARDOWN_CLEAN, NAMESPACE_GONE),
    )


def _pairing(tracker: str, code_host: str, *, installed: bool) -> Pairing:
    return Pairing(tracker, code_host, installed, (_loop_step(tracker, code_host),))


def _gitlab_step(target: str) -> Step:
    """The forge-loop step of gitlab+gitlab@<target> (#4399 decision 27)."""

    gitlab = _gitlab()
    key = f"gitlab+gitlab@{target}"
    inputs = {
        "tracker": "GitLab (gitlab), issues on the same project",
        "code_host": "GitLab (gitlab)",
        "ticket": (
            "--issue-file, else the default loop ticket (on the fixture: fix add() in widget.sh)"
        ),
        "review": "--revision-file, else the default review text (one follow-up change)",
        "candidate": "--candidate and --candidate-images (build for a pull request head)",
        "kube_context": "--context, else CURIE_FACTORY_KUBE_CONTEXT",
    }
    tracker: tuple[tuple[str, Any], ...] = (
        ("scenario.tracker.closes_issue", True),
        ("scenario.tracker.thread_replies", 1),
    )
    teardown_steps: tuple[str, ...] = (
        "reset fixture repository",
        "restore fixture default branch",
        "delete namespaces",
    )
    if target == "fixture":
        inputs["credentials"] = (
            "CURIE_FACTORY_* variables for the cluster, the layer registry and a real model "
            "(CURIE_FACTORY_MODEL_API_KEY); the GitLab CE fixture is brought up for the run "
            "with `forge-fixture gitlab up --state-dir <fresh dir> --advertise-host <the "
            "InternalIP of the kube context node running the Docker daemon>`, and removed "
            "after it; the factory token is the fixture's service account token"
        )
        remote = (
            "GitLab shows one new issue opened and labelled by developer through Sudo",
            "a second issue labelled by reporter admits no WorkItem and gets no state label",
            "GitLab shows exactly one merge request opened by the factory whose description "
            "carries Closes #<iid>",
            "the merge request's first head carries a failing external commit status from "
            "developer (CI red)",
            "a later head pushed by the factory has passing checks (CI green)",
            "a diff comment from developer is followed by a new commit on the same merge "
            "request and exactly one factory thread reply",
            "root merged the merge request and GitLab reads it merged",
        )
        tracker += (
            ("scenario.tracker.reporter_label_admitted", False),
            ("scenario.tracker.reporter_state_labels", []),
        )
        teardown_steps += (gitlab.FIXTURE_DOWN_STEP,)
        checks = (TEARDOWN_CLEAN, NAMESPACE_GONE, FIXTURE_GONE)
    else:
        project, factory_token, human_token = gitlab.SAAS_VARS
        inputs["credentials"] = (
            "CURIE_FACTORY_* variables for the cluster, the layer registry and a real model "
            f"(CURIE_FACTORY_MODEL_API_KEY); {project} names the gitlab.com test group's "
            f"subgroup project, {factory_token} the factory's Developer project access "
            f"token file and {human_token} the human Owner token file that labels, "
            "reviews, sets CI and merges"
        )
        remote = (
            "gitlab.com shows one new issue opened and labelled by the human Owner",
            "gitlab.com shows exactly one merge request opened by the factory whose "
            "description carries Closes #<iid>",
            "the merge request's first head carries a failing external commit status from "
            "the human Owner (CI red)",
            "a later head pushed by the factory has passing checks (CI green)",
            "a diff comment from the human Owner is followed by a new commit on the same "
            "merge request and exactly one factory thread reply",
            "the human Owner merged the merge request and gitlab.com reads it merged",
        )
        checks = (TEARDOWN_CLEAN, NAMESPACE_GONE)
    persisted = _persisted("forge-loop", "gitlab", key, teardown_steps)
    # The tracker assertions sit with the loop's, before the teardown ones.
    split = persisted.index(("teardown_clean", True))
    return Step(
        name=f"forge-loop {key}",
        inputs=inputs,
        command=["run", "--scenario", "forge-loop", "--forge", "gitlab", "--forge-target", target],
        deadline_seconds=LOOP_DEADLINE_SECONDS,
        remote_assertions=remote,
        persisted_assertions=(*persisted[:split], *tracker, *persisted[split:]),
        teardown_checks=checks,
    )


def _gitlab_pairing(target: str) -> Pairing:
    return Pairing("gitlab", "gitlab", True, (_gitlab_step(target),), target=target)


PAIRINGS: dict[str, Pairing] = {
    p.key: p
    for p in (
        _pairing("github", "github", installed=True),
        _pairing("jira_cloud", "github", installed=True),
        _gitlab_pairing("fixture"),
        _gitlab_pairing("saas"),
        _pairing("jira_cloud", "gitlab", installed=False),
        _pairing("jira_cloud", "bitbucket_cloud", installed=True),
        _pairing("jira_cloud", "bitbucket_dc", installed=True),
    )
}


def _is_unset(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)):
        return value <= 0
    try:
        return len(value) == 0
    except TypeError:
        return False


def unset_fields(pairing: Pairing) -> list[str]:
    """Every step field the entry leaves unset, one line each. Pure."""

    if not pairing.steps:
        return [f"{pairing.key}: steps is unset"]
    unset: list[str] = []
    for index, step in enumerate(pairing.steps, 1):
        label = f"{pairing.key} step {index} ({step.name or 'unnamed'})"
        for field in dataclasses.fields(Step):
            if _is_unset(getattr(step, field.name)):
                unset.append(f"{label}: {field.name} is unset")
    return unset


def _missing_drivers(pairing: Pairing) -> list[str]:
    fe.load_forge_entries()
    missing = []
    for name in dict.fromkeys((pairing.tracker, pairing.code_host)):
        entry = fe.FORGES.get(name)
        if entry is None or not entry.installed:
            missing.append(name)
    return missing


def _not_installed(pairing: Pairing) -> str:
    missing = _missing_drivers(pairing)
    if not missing:
        return f"{pairing.key} is not installed: no install path sets up this pairing yet"
    if len(missing) == 1:
        return f"{pairing.key} is not installed: the {missing[0]} forge driver is not installed"
    drivers = " and ".join(missing)
    return f"{pairing.key} is not installed: the {drivers} forge drivers are not installed"


def dry_run(key: str, out: TextIO) -> int:
    """List every step of one entry and its install state. Launches nothing."""

    pairing = PAIRINGS[key]
    state = "installed" if pairing.installed else _not_installed(pairing)
    print(f"acceptance {key}: tracker {pairing.tracker}, code host {pairing.code_host}", file=out)
    print(f"  {state}", file=out)
    for index, step in enumerate(pairing.steps, 1):
        print(f"  step {index}: {step.name}", file=out)
        print(
            f"    command: python3 {FACTORY_E2E.name} {' '.join(step.command)} "
            "<shared flags> --evidence <step evidence>",
            file=out,
        )
        print(f"    deadline: {step.deadline_seconds} seconds", file=out)
        print("    inputs:", file=out)
        for name, value in step.inputs.items():
            print(f"      {name}: {value}", file=out)
        print("    remote assertions:", file=out)
        for item in step.remote_assertions:
            print(f"      {item}", file=out)
        print("    persisted assertions:", file=out)
        for field, expected in step.persisted_assertions:
            print(f"      {field} == {json.dumps(expected)}", file=out)
        print("    teardown checks:", file=out)
        for item in step.teardown_checks:
            print(f"      {item}", file=out)
    unset = unset_fields(pairing)
    if unset:
        for line in unset:
            print(f"  unset: {line}", file=out)
        return fe.EXIT_CONFIG
    if not pairing.installed:
        return fe.EXIT_SCENARIO
    return 0


def default_runner(
    argv: list[str], timeout: float, *, grace: float = TEARDOWN_GRACE_SECONDS
) -> int:
    """Run one step; at its deadline SIGTERM it so it tears down, then kill it.

    The step overran either way, so TimeoutExpired is raised once it has exited:
    after its own teardown within ``grace`` seconds, or after the kill.
    """

    proc = subprocess.Popen(argv)
    try:
        return proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=grace)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        raise subprocess.TimeoutExpired(argv, timeout) from None


def default_kubectl(argv: list[str]) -> subprocess.CompletedProcess[str]:
    return subprocess.run(argv, capture_output=True, text=True, check=False, timeout=60)


def _shared_flags(args: Any) -> list[str]:
    flags = ["--candidate-images", args.candidate_images]
    for flag, value in (
        ("--context", args.context),
        ("--candidate", args.candidate),
        ("--kind-cluster", args.kind_cluster),
        ("--namespace", args.namespace),
        ("--issue-file", args.issue_file),
        ("--revision-file", args.revision_file),
        ("--model-base-url", args.model_base_url),
    ):
        if value is not None:
            flags += [flag, str(value)]
    return [*flags, "--admission-timeout", str(args.admission_timeout)]


_MISSING = object()


def _get_path(evidence: Any, dotted: str) -> Any:
    node = evidence
    for part in dotted.split("."):
        if not isinstance(node, dict) or part not in node:
            return _MISSING
        node = node[part]
    return node


def _credential_expiry_failures(field: str, got: Any) -> list[str]:
    """Every way the recorded credential rows fall short of the expiry labels.

    Pure. A release may record no forge credential, so an empty list passes;
    each recorded row must be an object whose ``expiry`` is one of
    ``CREDENTIALS_EXPIRY_LABELS`` (#4866).
    """

    if got is _MISSING:
        return [f"evidence {field} is missing, expected the cluster status credential rows"]
    if not isinstance(got, list):
        return [f"evidence {field} is {got!r}, expected the cluster status credential rows"]
    failures: list[str] = []
    for index, row in enumerate(got):
        if not isinstance(row, dict) or row.get("expiry") not in CREDENTIALS_EXPIRY_LABELS:
            failures.append(
                f"evidence {field}[{index}] is {row!r}, "
                f"expected expiry one of {', '.join(CREDENTIALS_EXPIRY_LABELS)}"
            )
    return failures


def _namespace_gone(args: Any, evidence: Mapping[str, Any], kubectl: Kubectl) -> str | None:
    """None when the namespace is gone, else why not.

    ``--ignore-not-found -o name`` makes absence an exit 0 with empty output, so
    any nonzero exit (an unknown context, an unreachable or forbidden API) is a
    failed check, never read as gone.
    """

    namespace = evidence.get("namespace") or args.namespace
    context = (
        args.context or evidence.get("kube_context") or os.environ.get("CURIE_FACTORY_KUBE_CONTEXT")
    )
    if not namespace or not context:
        return "cannot check the namespace: the evidence names no namespace or kube context"
    result = kubectl(
        [
            "kubectl",
            "--context",
            str(context),
            "get",
            "namespace",
            str(namespace),
            "--ignore-not-found",
            "-o",
            "name",
        ]
    )
    if result.returncode != 0:
        return f"could not read namespace {namespace}: {(result.stderr or '').strip()}"
    if (result.stdout or "").strip():
        return f"namespace {namespace} still exists on context {context}"
    return None


def _fixture_gone(evidence: Mapping[str, Any]) -> str | None:
    """None when the run's GitLab fixture state directory is gone, else why not."""

    recorded = _get_path(evidence, "gitlab_fixture.state_dir")
    if not isinstance(recorded, str) or not recorded:
        return "cannot check the GitLab fixture: the evidence names no gitlab_fixture.state_dir"
    if Path(recorded).exists():
        return f"the GitLab fixture state dir {recorded} still exists: the fixture was not removed"
    return None


def _judge_step(
    step: Step, args: Any, code: int, evidence: Mapping[str, Any] | None, kubectl: Kubectl
) -> list[str]:
    failures: list[str] = []
    if code != 0:
        failures.append(f"exited {code}")
    if evidence is None:
        failures.append("wrote no evidence")
        evidence = {}
    else:
        for field, expected in step.persisted_assertions:
            got = _get_path(evidence, field)
            if expected == CREDENTIALS_EXPIRY_VALID:
                failures.extend(_credential_expiry_failures(field, got))
            elif got is _MISSING:
                failures.append(f"evidence {field} is missing, expected {expected!r}")
            elif got != expected:
                failures.append(f"evidence {field} is {got!r}, expected {expected!r}")
    for check in step.teardown_checks:
        if check == TEARDOWN_CLEAN:
            if evidence.get("teardown_clean") is not True:
                failures.append("teardown is not clean")
        elif check == NAMESPACE_GONE:
            reason = _namespace_gone(args, evidence, kubectl)
            if reason:
                failures.append(reason)
        elif check == FIXTURE_GONE:
            reason = _fixture_gone(evidence)
            if reason:
                failures.append(reason)
        else:
            failures.append(f"no teardown check is defined for {check!r}")
    return failures


def _read_evidence(path: Path) -> dict[str, Any] | None:
    try:
        parsed = json.loads(path.read_text())
    except (OSError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def run(args: Any, *, runner: Runner = default_runner, kubectl: Kubectl = default_kubectl) -> int:
    """Run every step of one entry, judge it, and write one summary."""

    pairing = PAIRINGS[args.pairing]
    if not pairing.installed:
        print(f"factory-e2e: {_not_installed(pairing)}", file=sys.stderr)
        return fe.EXIT_SCENARIO
    unset = unset_fields(pairing)
    if unset:
        print(f"factory-e2e: {'; '.join(unset)}", file=sys.stderr)
        return fe.EXIT_CONFIG
    if args.hold:
        print("factory-e2e: acceptance does not take --hold", file=sys.stderr)
        return fe.EXIT_CONFIG
    if pairing.code_host == "gitlab" and pairing.target == "saas":
        missing = _gitlab().missing_saas_env(os.environ)
        if missing:
            print(f"factory-e2e: {pairing.key} needs {missing}", file=sys.stderr)
            return fe.EXIT_CONFIG
    if pairing.code_host == "gitlab" and pairing.target == "fixture":
        fixture_var = _gitlab().FIXTURE_VAR
        if os.environ.pop(fixture_var, None):
            # The entry proves a fixture it brings up and removes; never a reused one.
            fe.log(f"acceptance {pairing.key}: ignoring {fixture_var}; the run brings its own")

    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%M%SZ")
    base = args.evidence.parent if args.evidence else fe._repo_root() / "target" / "factory-e2e"
    shared = _shared_flags(args)
    results: list[dict[str, Any]] = []
    passed = True
    for index, step in enumerate(pairing.steps, 1):
        evidence_path = base / f"acceptance-{pairing.key}-{index}-{stamp}.json"
        argv = [
            sys.executable,
            str(FACTORY_E2E),
            *step.command,
            *shared,
            "--evidence",
            str(evidence_path),
        ]
        fe.log(f"acceptance {pairing.key}: step {index} {step.name}")
        started = time.monotonic()
        assert step.deadline_seconds is not None
        try:
            code = runner(argv, step.deadline_seconds)
            failures = _judge_step(step, args, code, _read_evidence(evidence_path), kubectl)
        except subprocess.TimeoutExpired:
            code = None
            failures = [f"overran its {step.deadline_seconds} second deadline"]
            failures += _judge_step(step, args, 0, _read_evidence(evidence_path), kubectl)
        results.append(
            {
                "step": step.name,
                "command": step.command,
                "exit_code": code,
                "evidence": str(evidence_path),
                "elapsed_seconds": round(time.monotonic() - started, 1),
                "verdict": "passed" if not failures else "failed",
                "failures": failures,
            }
        )
        if failures:
            passed = False
            break
    summary = {
        "schema": SCHEMA,
        "pairing": pairing.key,
        "result": "passed" if passed else "failed",
        "steps": results,
    }
    if args.evidence:
        args.evidence.parent.mkdir(parents=True, exist_ok=True)
        args.evidence.write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))
    return 0 if passed else fe.EXIT_FAILED
