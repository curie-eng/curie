"""Regression coverage for secret scan scope."""

from __future__ import annotations

import json
import os
import re
import secrets
import string
import subprocess
import tomllib
from pathlib import Path, PurePosixPath
from typing import Any, cast

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[3]
WORKFLOW_PATH = REPO_ROOT / ".github" / "workflows" / "gitleaks.yaml"
GITLEAKS_CONFIG_PATH = REPO_ROOT / ".gitleaks.toml"


def _workflow() -> dict[str, Any]:
    document = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    assert isinstance(document, dict)
    return document


def _gitleaks_job() -> dict[str, Any]:
    job = _workflow()["jobs"]["gitleaks"]
    assert isinstance(job, dict)
    return job


def _gitleaks_image() -> str:
    image = _gitleaks_job()["env"]["GITLEAKS_IMAGE"]
    assert isinstance(image, str)
    assert "@sha256:" in image, "the test must run the immutable workflow image"
    return image


def _trigger_block() -> dict[str, Any]:
    # PyYAML resolves the bare `on:` key to the boolean True.
    document = cast(dict[Any, Any], _workflow())
    triggers = document.get(True, document.get("on"))
    assert isinstance(triggers, dict)
    return triggers


def _push_scope_step() -> dict[str, Any]:
    steps = _gitleaks_job()["steps"]
    assert isinstance(steps, list)
    scope_steps = [
        step
        for step in steps
        if "GITLEAKS_LOG_OPTS=" in str(step.get("run", ""))
        and "github.sha" in str(step.get("run", ""))
    ]
    assert len(scope_steps) == 1, "workflow must define exactly one push scoping step"
    step = scope_steps[0]
    assert isinstance(step, dict)
    return step


def _push_log_range(pushed_sha: str) -> str:
    scope_command = str(_push_scope_step()["run"])
    match = re.search(r"GITLEAKS_LOG_OPTS=--log-opts=(?P<range>[^\"\n]+)", scope_command)
    assert match is not None, f"the push scoping step must export a log range: {scope_command}"
    return re.sub(r"\$\{\{\s*github\.sha\s*\}\}", pushed_sha, match.group("range")).strip()


def _scan_step() -> dict[str, Any]:
    steps = _gitleaks_job()["steps"]
    assert isinstance(steps, list)
    step = next(step for step in steps if step.get("name") == "Run gitleaks")
    assert isinstance(step, dict)
    return step


def _scan_report_path() -> str:
    scan_command = str(_scan_step()["run"])
    match = re.search(r"--report-path\s+(?P<path>\S+)", scan_command)
    assert match is not None, f"the scan step must write a findings report: {scan_command}"
    return match.group("path")


def _attribution_step() -> dict[str, Any]:
    steps = _gitleaks_job()["steps"]
    assert isinstance(steps, list)
    attribution_steps = [
        step
        for step in steps
        if "--contains" in str(step.get("run", ""))
        and re.search(r"git (for-each-ref|branch)\b", str(step.get("run", "")))
    ]
    assert len(attribution_steps) == 1, "workflow must define exactly one ref attribution step"
    step = attribution_steps[0]
    assert isinstance(step, dict)
    return step


def _gitleaks_config() -> dict[str, Any]:
    with GITLEAKS_CONFIG_PATH.open("rb") as handle:
        document = tomllib.load(handle)
    assert isinstance(document, dict)
    return document


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=repo,
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _commit(repo: Path, message: str) -> str:
    _git(repo, "add", "--all")
    _git(repo, "commit", "--message", message)
    return _git(repo, "rev-parse", "HEAD")


def _new_repo(path: Path) -> tuple[Path, str]:
    path.mkdir()
    _git(path, "init", "--initial-branch", "main")
    _git(path, "config", "user.name", "Scope Test")
    _git(path, "config", "user.email", "scope-test@example.invalid")
    (path / "README.txt").write_text("safe\n", encoding="utf-8")
    return path, _commit(path, "initial commit")


def _runtime_secret() -> str:
    alphabet = string.ascii_uppercase + string.digits
    suffix = "".join(secrets.choice(alphabet) for _ in range(36))
    return "GITHUB_TOKEN=" + "gh" + "p_" + suffix + "\n"


def _scan(
    repo: Path,
    log_range: str | None = None,
    report_path: str | None = None,
) -> subprocess.CompletedProcess[str]:
    mount = f"{repo}:/repo" if report_path is not None else f"{repo}:/repo:ro"
    command = [
        "docker",
        "run",
        "--rm",
        "--user",
        f"{os.getuid()}:{os.getgid()}",
        "--volume",
        mount,
        _gitleaks_image(),
        "detect",
        "--source",
        "/repo",
        "--redact",
        "--verbose",
        "--exit-code",
        "1",
    ]
    # A same-version native release lets contributors exercise the real scanner
    # without starting Docker. CI still uses the workflow's immutable image.
    if binary := os.environ.get("GITLEAKS_TEST_BINARY"):
        version = subprocess.run(
            [binary, "version"], check=True, capture_output=True, text=True
        ).stdout.strip()
        image_version = _gitleaks_image().split(":v", 1)[1].split("@", 1)[0]
        assert version == image_version, "native scanner must match the pinned CI version"
        command = [binary, "detect", "--source", str(repo), "--redact", "--verbose",
                   "--exit-code", "1"]
        if report_path is not None:
            report_path = str(repo / PurePosixPath(report_path).relative_to("/repo"))
    if report_path is not None:
        command.extend(["--report-path", report_path])
    if log_range is not None:
        command.append(f"--log-opts={log_range}")
    return subprocess.run(command, capture_output=True, text=True, check=False)


def _scan_diagnostics(result: subprocess.CompletedProcess[str]) -> str:
    return f"exit code: {result.returncode}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"


def test_pull_request_range_ignores_secret_on_unrelated_branch(tmp_path: Path) -> None:
    repo, base_sha = _new_repo(tmp_path / "repo")

    _git(repo, "switch", "--create", "unrelated")
    (repo / "credentials.env").write_text(_runtime_secret(), encoding="utf-8")
    _commit(repo, "unrelated secret")

    _git(repo, "switch", "--create", "pull-request", base_sha)
    (repo / "feature.txt").write_text("clean pull request\n", encoding="utf-8")
    head_sha = _commit(repo, "clean pull request")

    result = _scan(repo, f"{base_sha}..{head_sha}")

    assert result.returncode == 0, _scan_diagnostics(result)


def test_pull_request_range_finds_secret_in_earlier_pull_request_commit(
    tmp_path: Path,
) -> None:
    repo, base_sha = _new_repo(tmp_path / "repo")
    _git(repo, "switch", "--create", "pull-request", base_sha)

    secret_path = repo / "credentials.env"
    secret_path.write_text(_runtime_secret(), encoding="utf-8")
    _commit(repo, "add secret")
    secret_path.unlink()
    head_sha = _commit(repo, "remove secret")

    result = _scan(repo, f"{base_sha}..{head_sha}")

    assert result.returncode == 1, _scan_diagnostics(result)


def test_unrestricted_scan_finds_secret_in_main_history(tmp_path: Path) -> None:
    repo, _ = _new_repo(tmp_path / "repo")

    secret_path = repo / "credentials.env"
    secret_path.write_text(_runtime_secret(), encoding="utf-8")
    _commit(repo, "add secret")
    secret_path.unlink()
    _commit(repo, "remove secret")

    result = _scan(repo)

    assert result.returncode == 1, _scan_diagnostics(result)


def test_workflow_limits_log_range_to_pull_requests() -> None:
    job = _gitleaks_job()
    assert job["name"] == "gitleaks (full history)"
    steps = job["steps"]
    assert isinstance(steps, list)

    checkout = next(step for step in steps if step.get("uses") == "actions/checkout@v7")
    assert checkout["with"]["fetch-depth"] == 0

    scope_steps = [
        step
        for step in steps
        if "--log-opts" in str(step.get("run", ""))
        and "github.event.pull_request.base.sha" in str(step.get("run", ""))
        and "github.event.pull_request.head.sha" in str(step.get("run", ""))
    ]
    assert len(scope_steps) == 1, "workflow must define one pull request base to head range"

    scope_step = scope_steps[0]
    condition = str(scope_step.get("if", "")).removeprefix("${{").removesuffix("}}").strip()
    assert condition in {
        "github.event_name == 'pull_request'",
        'github.event_name == "pull_request"',
    }, "the log range must be configured only for pull request events"

    scope_command = str(scope_step["run"])
    assert re.search(
        r"--log-opts=.*github\.event\.pull_request\.base\.sha.*"
        r"\.\..*github\.event\.pull_request\.head\.sha",
        scope_command,
        re.DOTALL,
    ), "the pull request range must run from the event base SHA to the event head SHA"
    assert "GITHUB_ENV" in scope_command

    scan_step = next(step for step in steps if step.get("name") == "Run gitleaks")
    assert "if" not in scan_step, "the unrestricted scan must still run for full history events"
    scan_command = str(scan_step["run"])
    assert re.search(
        r"docker run.*?detect .*?\$\{GITLEAKS_LOG_OPTS:\+\"\$GITLEAKS_LOG_OPTS\"\}",
        scan_command,
        re.DOTALL,
    ), "the gitleaks invocation must pass the pull request range only when it is set"


def test_workflow_limits_log_range_to_the_pushed_branch_history() -> None:
    step = _push_scope_step()

    condition = str(step.get("if", "")).removeprefix("${{").removesuffix("}}").strip()
    assert condition in {
        "github.event_name == 'push'",
        'github.event_name == "push"',
    }, "the push log range must be configured only for push events"

    scope_command = str(step["run"])
    assert re.search(
        r"GITLEAKS_LOG_OPTS=--log-opts=[^\"\n]*\$\{\{\s*github\.sha\s*\}\}",
        scope_command,
    ), "the push range must be anchored on the pushed commit SHA"
    assert "GITHUB_ENV" in scope_command


def test_scheduled_and_manual_scans_stay_unscoped() -> None:
    steps = _gitleaks_job()["steps"]
    assert isinstance(steps, list)

    setters = [step for step in steps if "GITLEAKS_LOG_OPTS=" in str(step.get("run", ""))]
    assert setters, "the workflow must scope at least one event"
    for step in setters:
        condition = str(step.get("if", ""))
        assert "pull_request" in condition or "push" in condition, (
            f"step {step.get('name')!r} narrows the log range without a pull request or push "
            "guard, which would also narrow the scheduled sweep and manual dispatches"
        )

    triggers = _trigger_block()
    assert "schedule" in triggers, "the weekly full history sweep must stay configured"
    assert "workflow_dispatch" in triggers, "the manual full history scan must stay configured"


def test_push_range_ignores_secret_on_unmerged_branch(tmp_path: Path) -> None:
    repo, _ = _new_repo(tmp_path / "repo")

    _git(repo, "switch", "--create", "unmerged")
    (repo / "credentials.env").write_text(_runtime_secret(), encoding="utf-8")
    _commit(repo, "unmerged secret")

    _git(repo, "switch", "main")
    (repo / "feature.txt").write_text("clean release branch\n", encoding="utf-8")
    pushed_sha = _commit(repo, "clean release branch commit")

    scoped = _scan(repo, _push_log_range(pushed_sha))
    assert scoped.returncode == 0, _scan_diagnostics(scoped)

    unscoped = _scan(repo)
    assert unscoped.returncode == 1, _scan_diagnostics(unscoped)


def test_push_range_finds_secret_deleted_from_pushed_branch_history(tmp_path: Path) -> None:
    repo, _ = _new_repo(tmp_path / "repo")

    secret_path = repo / "credentials.env"
    secret_path.write_text(_runtime_secret(), encoding="utf-8")
    _commit(repo, "add secret")
    secret_path.unlink()
    pushed_sha = _commit(repo, "remove secret")

    result = _scan(repo, _push_log_range(pushed_sha))

    assert result.returncode == 1, _scan_diagnostics(result)


def test_gitleaks_config_declares_no_rewritten_history_stopwords() -> None:
    stopwords = _gitleaks_config()["allowlist"]["stopwords"]
    assert isinstance(stopwords, list)

    # Split so no literal here reaches the slack-conversation-id shape
    # (\b[CGD][0-9][A-Z0-9]{7,9}\b): "C000000S" alone is only 8 chars and
    # does not match. Do not rejoin this into a single literal.
    dead_prefix = "C000000S"
    dead_pattern = re.compile(re.escape(dead_prefix) + r"0[6-9]")
    dead = [word for word in stopwords if dead_pattern.fullmatch(str(word))]

    assert dead == [], f"these stopwords match nothing after the history rewrite: {dead}"


def test_gitleaks_config_keeps_the_slack_identifier_guards() -> None:
    config = _gitleaks_config()

    slack_rule = next(rule for rule in config["rules"] if rule["id"] == "slack-conversation-id")
    assert slack_rule["regex"] == r"\b[CGD][0-9][A-Z0-9]{7,9}\b"

    # Read the allowlist for what it COVERS, not for one literal spelling. The
    # CLI validates all three conversation prefixes, so a test needing a fake
    # DM or private group id has to be able to write one without reddening the
    # scan, and nothing that merely looks like an id may ride along.
    rule = re.compile(str(slack_rule["regex"]))
    allowed = [re.compile(str(pattern)) for pattern in config["allowlist"]["regexes"]]

    for example in ("C0EXAMPLE1", "D0EXAMPLE1", "G0EXAMPLE1"):
        assert rule.search(example), f"{example} is not the shape the rule catches"
        assert any(pattern.search(example) for pattern in allowed), (
            f"{example} is not allowlisted, so any test using it reddens the scan"
        )

    # Split so no literal here reaches the rule's shape; rejoining is the defect
    # this file's sibling test already had to fix once.
    not_an_example = "C0" + "NOTLISTED"
    assert rule.search(not_an_example), "the negative control must be a real match first"
    assert not any(pattern.search(not_an_example) for pattern in allowed), (
        "the EXAMPLE allowlist must not wave through an ordinary looking id"
    )


def test_scan_step_writes_the_report_the_attribution_step_reads() -> None:
    scan_step = _scan_step()
    assert scan_step.get("id"), "the scan step needs an id so a later step can key off its outcome"

    report_path = _scan_report_path()
    attribution_command = str(_attribution_step()["run"])

    assert PurePosixPath(report_path).name in attribution_command, (
        f"the attribution step must read the report the scan writes ({report_path})"
    )


def test_workflow_names_the_branch_each_finding_came_from() -> None:
    scan_id = str(_scan_step().get("id", ""))
    assert scan_id, "the scan step needs an id so a later step can key off its outcome"

    step = _attribution_step()
    condition = str(step.get("if", ""))
    assert "failure()" in condition, "attribution must not fire on a green run"
    assert re.search(
        rf"steps\.{re.escape(scan_id)}\.(outcome|conclusion)\s*==\s*['\"]failure['\"]",
        condition,
    ), "attribution must be conditioned on the scan step failing"

    attribution_command = str(step["run"])
    assert PurePosixPath(_scan_report_path()).name in attribution_command
    assert re.search(r"git for-each-ref[^\n]*--contains", attribution_command), (
        "attribution must map each reported commit to the refs that contain it"
    )
    # `git branch --all --contains` lists branch refs only, so a finding on a
    # commit reachable only from a tag printed a bare colon. The wide sweep
    # reaches tags, so the attribution has to as well.
    assert "refs/tags" in attribution_command, (
        "attribution must cover tags, not only branches, because the scheduled "
        "sweep scans every ref the checkout pulled down"
    )
    assert "refs/heads" in attribution_command and "refs/remotes" in attribution_command, (
        "attribution must still cover local and remote-tracking branches"
    )


def test_attribution_command_names_the_branch_a_finding_came_from(tmp_path: Path) -> None:
    repo, _ = _new_repo(tmp_path / "repo")

    _git(repo, "switch", "--create", "feature/leaky")
    (repo / "credentials.env").write_text(_runtime_secret(), encoding="utf-8")
    leaky_sha = _commit(repo, "leaky commit")
    _git(repo, "switch", "main")

    scan = _scan(repo, report_path=_scan_report_path())
    assert scan.returncode == 1, _scan_diagnostics(scan)

    attribution = subprocess.run(
        ["bash", "-c", str(_attribution_step()["run"])],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )

    assert attribution.returncode == 0, _scan_diagnostics(attribution)
    assert leaky_sha in attribution.stdout, _scan_diagnostics(attribution)
    assert "feature/leaky" in attribution.stdout, _scan_diagnostics(attribution)


def test_attribution_command_names_the_tag_a_finding_came_from(tmp_path: Path) -> None:
    """A finding reachable only from a tag must still be attributed to that tag.

    The scheduled sweep walks every ref the full checkout pulled down, tags
    included, and this repository carries release tags plus `backup/*` refs. A
    branch-only attribution printed the SHA followed by an empty list once the
    branch was deleted, which is the non-actionable output the step exists to
    prevent.
    """
    repo, _ = _new_repo(tmp_path / "repo")

    _git(repo, "switch", "--create", "doomed")
    (repo / "credentials.env").write_text(_runtime_secret(), encoding="utf-8")
    leaky_sha = _commit(repo, "leaky commit")
    _git(repo, "tag", "v9.9.9-leaky", leaky_sha)
    _git(repo, "switch", "main")
    _git(repo, "branch", "--delete", "--force", "doomed")

    scan = _scan(repo, report_path=_scan_report_path())
    assert scan.returncode == 1, _scan_diagnostics(scan)

    attribution = subprocess.run(
        ["bash", "-c", str(_attribution_step()["run"])],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )

    assert attribution.returncode == 0, _scan_diagnostics(attribution)
    assert leaky_sha in attribution.stdout, _scan_diagnostics(attribution)
    assert "v9.9.9-leaky" in attribution.stdout, _scan_diagnostics(attribution)
    assert not re.search(rf"{re.escape(leaky_sha)} appears on:\s*$", attribution.stdout, re.M), (
        f"the finding was reported with no ref named: {attribution.stdout}"
    )


@pytest.mark.parametrize(
    ("owner", "name", "suffix", "expected_rules"),
    [
        ("curie-eng", "acme-private", "", {"downstream-repo-slug"}),
        ("Curie-Eng", "acme-private", "", {"downstream-repo-slug"}),
        ("CURIE-ENG", "acme-secret-agent", "", {"downstream-repo-slug"}),
        ("curie-eng", "acme-private", "#123", {"downstream-repo-slug", "cross-repo-issue-ref"}),
        ("Curie-Eng", "acme-private", "#123", {"downstream-repo-slug", "cross-repo-issue-ref"}),
        ("acme-corp", "acme-bot", "#123", {"cross-repo-issue-ref"}),
        ("curie-eng", "curie", "#123", set()),
        ("Curie-Eng", "Curie", "#123", set()),
        ("CURIE-ENG", "AGENTOS", "#123", set()),
        ("curie-eng", "curie", ".git", set()),
        ("Curie-Eng", "Curie", ".git", set()),
        ("CURIE-ENG", "AGENTOS", ".git", set()),
        ("curie-eng", "curie", ".", set()),
        ("curie-eng", "curie", "-{runner,api}", set()),
        ("Curie-Eng", "Curie-runner", "", set()),
        ("curie-eng", "curie", ".git-extra", {"downstream-repo-slug"}),
        ("curie-eng", "curie", ".git.other", {"downstream-repo-slug"}),
        ("#456", "PR", "#503", set()),
    ],
)
def test_repository_identifier_rules_preserve_case_and_clone_verdicts(
    tmp_path: Path, owner: str, name: str, suffix: str, expected_rules: set[str]
) -> None:
    """Use the pinned scanner, including its RE2 match and allowlist semantics.

    Every non-platform name is invented. Build the owner/name form at runtime
    so the corpus does not itself name a forbidden downstream repository.
    """
    repo, base_sha = _new_repo(tmp_path / "repo")
    (repo / ".gitleaks.toml").write_bytes(GITLEAKS_CONFIG_PATH.read_bytes())
    slug = f"{owner}/{name}{suffix}"
    (repo / "identifiers.txt").write_text(
        f"{slug}\nhttps://github.com/{slug}\ngit@github.com:{slug}\n", encoding="utf-8"
    )
    head_sha = _commit(repo, "add invented identifier probes")

    scan = _scan(repo, f"{base_sha}..{head_sha}", report_path="/repo/findings.json")

    assert scan.returncode == (1 if expected_rules else 0), _scan_diagnostics(scan)
    findings = json.loads((repo / "findings.json").read_text(encoding="utf-8"))
    assert {finding["RuleID"] for finding in findings} == expected_rules
