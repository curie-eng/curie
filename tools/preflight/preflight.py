#!/usr/bin/env python3
"""Run the PR gates selected by a source checkout's committed diff."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shlex
import signal
import socket
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Iterator, Sequence
from contextlib import contextmanager, nullcontext
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Never
from urllib.parse import quote

import yaml


@dataclass(frozen=True)
class Check:
    workflow: str
    job: str
    step: str
    command: str
    cwd: str
    group: str
    ci_command: str


class TransientError(ValueError):
    """An unavailable operational dependency may recover with the same input."""


class GateFailure(ValueError):
    """A well-formed invocation reached a failing product verification gate."""


def pytest_command(tests: Sequence[str]) -> list[str]:
    # Matches CI's xdist scheduling; the root conftest.py groups tests for loadgroup.
    return ["uv", "run", "pytest", "-q", "-n", "4", "--dist", "loadgroup", *tests]


def released_upgrade_environment(
    project: str, compose_files: str, postgres_port: int
) -> dict[str, str]:
    # scripts/check-released-upgrade.py reads this isolation contract; INTEGRATION=1
    # makes a missing private Postgres fail instead of skip, as in the isolated baseline.
    return {
        "CURIE_RELEASED_UPGRADE_COMPOSE_PROJECT": project,
        "CURIE_RELEASED_UPGRADE_COMPOSE_FILES": compose_files,
        "CURIE_RELEASED_UPGRADE_POSTGRES_HOST": "127.0.0.1",
        "CURIE_RELEASED_UPGRADE_POSTGRES_PORT": str(postgres_port),
        "CURIE_RELEASED_UPGRADE_INTEGRATION": "1",
    }


def exact(job: str, step: str, command: str, cwd: str, group: str) -> Check:
    return Check("ci.yaml", job, step, command, cwd, group, command)


# These commands are declarations, independent of the workflows read by the
# drift guard. Updating CI requires an intentional update here and its tests.
ACTION_PINS_COMMAND = (
    r"""uv run --no-project --with pyyaml==6.0.3 python3 - <<'PY'
import re
from pathlib import Path

import yaml

def uses_nodes(node, seen):
    if id(node) in seen:
        return
    seen.add(id(node))
    if isinstance(node, yaml.MappingNode):
        for key, value in node.value:
            if isinstance(key, yaml.ScalarNode) and key.value == "uses":
                yield value
        children = (value for key, value in node.value)
    elif isinstance(node, yaml.SequenceNode):
        children = iter(node.value)
    else:
        children = ()
    for child in children:
        yield from uses_nodes(child, seen)

errors = []
checked = 0
directory = Path(".github/workflows")
for path in sorted((*directory.glob("*.yaml"), *directory.glob("*.yml"))):
    source = path.read_text()
    # Token gaps contain comments, unlike quoted scalars containing '#'.
    # YAML nodes also cover quoted keys, flow mappings, and aliases.
    comments = {}
    previous = 0
    for token in yaml.scan(source):
        gap = source[previous:token.start_mark.index]
        for match in re.finditer(r"#[^\n]*", gap):
            position = previous + match.start()
            comments[source.count("\n", 0, position)] = match.group()
        previous = token.end_mark.index
    root = yaml.compose(source, Loader=yaml.SafeLoader)
    for node in uses_nodes(root, set()):
        line = node.start_mark.line + 1
        if not isinstance(node, yaml.ScalarNode):
            errors.append((path, line, "uses must be a scalar action reference"))
            continue
        value = node.value
        # Local actions use this repository's checked out code.
        if not value.startswith(("actions/", "./")):
            checked += 1
            if not re.fullmatch(r"[^/\s@]+/[^\s@]+@[0-9a-fA-F]{40}", value):
                errors.append((path, line, "third party action requires a full 40 hex commit SHA"))
            comment = comments.get(node.end_mark.line, "")
            if not re.search(r"#\s*v[0-9]+(?:\.[0-9]+)*\b", comment):
                errors.append((path, line, "third party action requires a version comment"""
    r""" on its uses line"))
for path, line, message in errors:
    print(f"::error file={path},line={line}::{message}")
if errors:
    raise SystemExit(1)
print(f"Verified {checked} third party action references")
PY"""
)
COMMIT_COMMAND = r'''bash scripts/check-commit-messages.sh \
  "${{ github.event.pull_request.base.sha }}..${{ github.event.pull_request.head.sha }}"'''
GITLEAKS_COMMAND = r"""docker run --rm -v "${{ github.workspace }}:/repo" \
  "$GITLEAKS_IMAGE" \
  detect --source /repo --redact --verbose --exit-code 1 \
  --report-path /repo/gitleaks-report.json \
  ${GITLEAKS_LOG_OPTS:+"$GITLEAKS_LOG_OPTS"}"""
COMPILE_CI_COMMAND = "cargo test --locked"
COMPILE_FAST_COMMAND = "cargo check --locked --tests"
TYPESCRIPT_COMMAND = r"""npx --yes json-schema-to-typescript@15.0.4 \
  packages/aci-protocol/schema/aci-protocol.schema.json \
  --unreachableDefinitions \
  -o packages/aci-protocol/generated/ts/aci-protocol.ts
git diff --exit-code -- packages/aci-protocol/generated/ts/aci-protocol.ts"""
MANIFEST_COMMAND = (
    r"""pnpm gen:manifest
git diff --exit-code src/generated/commandManifest.ts \
  || (echo "commandManifest.ts is stale; run 'pnpm gen:manifest' in apps/ui"""
    r""" and commit." && exit 1)"""
)
# A fresh hook worktree has no node_modules. This is dependency setup from CI,
# not another selected gate, and --dry-run never runs it.
UI_SETUP = exact("ui", "Install dependencies", "pnpm install --frozen-lockfile", "apps/ui", "setup")
CHECKS = (
    exact(
        "action-pins",
        "Require third party action SHA pins and version comments",
        ACTION_PINS_COMMAND,
        ".",
        "always",
    ),
    exact("commit-messages", "Check the PR's commit messages", COMMIT_COMMAND, ".", "always"),
    Check(
        "gitleaks.yaml",
        "gitleaks",
        "Run gitleaks",
        GITLEAKS_COMMAND,
        ".",
        "always",
        GITLEAKS_COMMAND,
    ),
    exact("python", "Lockfile is up to date", "uv lock --check", ".", "python"),
    exact(
        "python",
        "Alembic revision gate",
        "uv run python scripts/check-alembic-revisions.py",
        ".",
        "python",
    ),
    exact(
        "python",
        "Schema window gate",
        "uv run python scripts/check-schema-window.py",
        ".",
        "python",
    ),
    exact("python", "Ruff", "uv run ruff check .", ".", "python"),
    exact("python", "Mypy", "uv run mypy", ".", "python"),
    exact(
        "python",
        "Import boundaries (harness SDK containment)",
        "uv run lint-imports",
        ".",
        "python",
    ),
    exact(
        "rust-lint", "Version consistency", "bash scripts/check-version-consistency.sh", ".", "rust"
    ),
    exact("rust-lint", "Fmt", "cargo fmt --check", "cli", "rust"),
    exact(
        "rust-lint", "Clippy", "cargo clippy --locked --all-targets -- -D warnings", "cli", "rust"
    ),
    exact(
        "rust-lint",
        "Schema baseline",
        "bash scripts/refresh-schema-baseline.sh --check",
        "cli",
        "rust",
    ),
    exact(
        "rust-lint",
        "Schema window gate",
        "uv run python scripts/check-schema-window.py",
        ".",
        "rust",
    ),
    Check(
        "ci.yaml",
        "rust-lint",
        "Generated ACI protocol crate compiles",
        COMPILE_FAST_COMMAND,
        "packages/aci-protocol/generated/rust",
        "rust",
        COMPILE_CI_COMMAND,
    ),
    exact(
        "contracts-ts",
        "Regenerate TypeScript from the committed schema and check for drift",
        TYPESCRIPT_COMMAND,
        ".",
        "contracts",
    ),
    exact(
        "contracts-ts",
        "tsc --noEmit on generated ACI types",
        "npx --yes -p typescript@5.9.3 tsc --noEmit -p "
        "packages/aci-protocol/generated/ts/tsconfig.json",
        ".",
        "contracts",
    ),
    exact("ui", "Command manifest is current", MANIFEST_COMMAND, "apps/ui", "cli"),
    exact("ui", "Lint", "pnpm lint", "apps/ui", "ui"),
)


def load_workflow(root: Path, filename: str) -> dict[str, Any]:
    try:
        workflow = yaml.safe_load((root / ".github/workflows" / filename).read_text())
    except (OSError, yaml.YAMLError) as error:
        raise ValueError(f"Cannot read workflow {filename}: {error}") from error
    if not isinstance(workflow, dict) or not isinstance(workflow.get("jobs"), dict):
        raise ValueError(f"Workflow {filename} has no jobs mapping")
    return workflow


def validate_workflows(root: Path) -> None:
    declarations = (*CHECKS, UI_SETUP)
    workflows = {
        name: load_workflow(root, name) for name in {check.workflow for check in declarations}
    }
    for check in declarations:
        label = f"{check.workflow}:{check.job}: {check.step}"
        job = workflows[check.workflow]["jobs"].get(check.job)
        if not isinstance(job, dict) or not isinstance(job.get("steps"), list):
            raise ValueError(f"Mirrored step is missing: {label}")
        steps = [
            step
            for step in job["steps"]
            if isinstance(step, dict) and step.get("name") == check.step
        ]
        if len(steps) != 1:
            raise ValueError(f"Mirrored step is missing or duplicated: {label}")
        actual = steps[0].get("run")
        if not isinstance(actual, str) or actual.strip() != check.ci_command.strip():
            raise ValueError(f"Mirrored CI command changed: {label}")
        if check.step == "Generated ACI protocol crate compiles":
            # Decision 10 pins both sides separately. A caller cannot change
            # the fast command merely by retaining the same CI declaration.
            if (
                check.ci_command != "cargo test --locked"
                or check.command != "cargo check --locked --tests"
            ):
                raise ValueError(f"Compile-only mapping changed: {label}")
        elif check.command.strip() != check.ci_command.strip():
            raise ValueError(f"Mirrored preflight command changed: {label}")
        defaults = job.get("defaults", {}).get("run", {})
        cwd = steps[0].get("working-directory", defaults.get("working-directory", "."))
        if cwd == "${{ github.workspace }}":
            cwd = "."
        if cwd != check.cwd:
            raise ValueError(f"Mirrored working directory changed: {label}")
    image = workflows["gitleaks.yaml"]["jobs"]["gitleaks"].get("env", {}).get("GITLEAKS_IMAGE")
    if not isinstance(image, str) or not re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", image):
        raise ValueError("gitleaks.yaml: GITLEAKS_IMAGE must name a digest-pinned image")


def selected_checks(paths: list[str]) -> list[Check]:
    groups = {"always"}
    for path in paths:
        parts = Path(path).parts
        if (
            path.endswith(".py")
            or Path(path).name in {"pyproject.toml", "uv.lock"}
            or any(part.startswith("alembic") for part in parts)
        ):
            groups.add("python")
        if path.startswith(("cli/", "packages/aci-protocol/")):
            groups.add("rust")
        if path.startswith("packages/aci-protocol/"):
            groups.add("contracts")
        if path.startswith("cli/"):
            groups.add("cli")
        if path.startswith("apps/ui/"):
            groups.add("ui")
    return [check for check in CHECKS if check.group in groups]


def git(root: Path, *args: str) -> str:
    try:
        result = subprocess.run(
            ["git", *args], cwd=root, text=True, capture_output=True, check=False
        )
    except OSError as error:
        raise TransientError("Cannot run Git for the source checkout") from error
    if result.returncode:
        raise ValueError(result.stderr.strip() or f"git {' '.join(args)} failed")
    return result.stdout.strip()


def changed_paths(root: Path, base: str, head: str) -> list[str]:
    result = subprocess.run(
        ["git", "diff", "--no-renames", "--name-only", "-z", f"origin/{base}...{head}"],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode:
        raise ValueError(result.stderr.strip() or "Cannot read the changed file paths")
    return [path for path in result.stdout.split("\0") if path]


def check_base_freshness(root: Path, base: str, head: str = "HEAD") -> dict[str, Any]:
    git(root, "check-ref-format", f"refs/heads/{base}")
    git(root, "remote", "get-url", "origin")
    try:
        fetched = subprocess.run(
            ["git", "fetch", "origin", base],
            cwd=root,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as error:
        raise TransientError("Cannot run Git to fetch the base") from error
    if fetched.returncode:
        if "couldn't find remote ref" in fetched.stderr:
            raise ValueError("The requested origin base reference does not exist")
        raise TransientError(f"Cannot fetch the origin base: {fetched.stderr.strip()}")
    result = subprocess.run(
        ["git", "merge-base", "--is-ancestor", f"origin/{base}", head],
        cwd=root,
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode not in (0, 1):
        raise ValueError(result.stderr.strip() or "Cannot compare the base tip to the head")
    contains_tip = result.returncode == 0
    return {
        "contains_tip": contains_tip,
        "detail": (
            f"The selected head contains origin/{base}"
            if contains_tip
            else f"The selected head does not contain origin/{base}"
        ),
        "fix": "git merge " + shlex.quote(f"origin/{base}"),
    }


def gh_json(root: Path, *args: str, allow_not_found: bool = False) -> Any:
    try:
        result = subprocess.run(
            ["gh", "api", *args],
            cwd=root,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as error:
        raise TransientError("Cannot run authenticated gh for the GitHub read") from error
    if result.returncode:
        if allow_not_found and "(HTTP 404)" in result.stderr:
            return None
        raise TransientError(f"GitHub read failed: {result.stderr.strip()}")
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError as error:
        raise ValueError("GitHub read did not return valid JSON") from error


def base_health(root: Path, base: str, repository: str) -> list[str]:
    # Effective branch rules include rulesets even when classic protection is
    # absent. https://docs.github.com/en/rest/repos/rules#get-rules-for-a-branch
    rules = gh_json(root, f"repos/{repository}/rules/branches/{quote(base, safe='')}")
    if not isinstance(rules, list):
        raise ValueError("GitHub branch rules did not return a list")
    required: set[str] = set()
    for rule in rules:
        if rule.get("type") == "required_status_checks":
            required.update(
                check["context"] for check in rule["parameters"]["required_status_checks"]
            )
    # Classic branch protection is an independent policy alongside rulesets.
    # A 404 means none; authentication and all other read failures are errors.
    # https://docs.github.com/en/rest/branches/branch-protection#get-status-checks-protection
    classic = gh_json(
        root,
        f"repos/{repository}/branches/{quote(base, safe='')}/protection/required_status_checks",
        allow_not_found=True,
    )
    if classic is not None:
        if not isinstance(classic, dict):
            raise ValueError("GitHub status check protection did not return an object")
        required.update(classic.get("contexts", []))
        required.update(check["context"] for check in classic.get("checks", []))
    tip = git(root, "rev-parse", f"origin/{base}")
    pages = gh_json(
        root,
        f"repos/{repository}/commits/{tip}/check-runs?per_page=100",
        "--paginate",
        "--slurp",
    )
    # --slurp wraps paginated objects. A single object is also the REST shape.
    if isinstance(pages, dict):
        pages = [pages]
    if not isinstance(pages, list) or any(
        not isinstance(page, dict) or not isinstance(page.get("check_runs"), list) for page in pages
    ):
        raise ValueError("GitHub check runs did not return a complete list")
    failing = {"failure", "cancelled", "timed_out", "action_required", "startup_failure", "stale"}
    return sorted(
        {
            check["name"]
            for page in pages
            for check in page["check_runs"]
            if check.get("name") in required and check.get("conclusion") in failing
        }
    )


def select_python_tests(root: Path, paths: list[str]) -> list[str]:
    manifest = tomllib.loads((root / "pyproject.toml").read_text())
    workspace = manifest["tool"]["uv"]["workspace"]
    excluded = {
        member.resolve()
        for pattern in workspace.get("exclude", [])
        for member in root.glob(pattern)
    }
    members: dict[str, tuple[Path, set[str]]] = {}

    def normalize(name: str) -> str:
        return re.sub(r"[-_.]+", "-", name).lower()

    for pattern in workspace["members"]:
        for member in sorted(root.glob(pattern)):
            if member.resolve() in excluded:
                continue
            project = tomllib.loads((member / "pyproject.toml").read_text())["project"]
            dependencies = set()
            for dependency in project.get("dependencies", []):
                match = re.match(r"[A-Za-z0-9][A-Za-z0-9_.-]*", dependency)
                if match:
                    dependencies.add(normalize(match.group()))
            members[normalize(project["name"])] = (member, dependencies)
    selected = {
        name
        for name, (member, _) in members.items()
        if any(
            path in {"pyproject.toml", "uv.lock"}
            or path == member.relative_to(root).as_posix()
            or path.startswith(member.relative_to(root).as_posix() + "/")
            for path in paths
        )
    }
    while True:
        dependents = {
            name for name, (_, dependencies) in members.items() if dependencies & selected
        }
        expanded = selected | dependents
        if expanded == selected:
            break
        selected = expanded
    if not selected:
        return []
    tests = {
        (members[name][0] / "tests").relative_to(root).as_posix()
        for name in selected
        if (members[name][0] / "tests").is_dir()
    }
    for line in (root / "tools/preflight/always.txt").read_text().splitlines():
        entry = line.split("#", 1)[0].strip()
        if entry:
            path = Path(entry)
            if path.is_absolute() or ".." in path.parts or not (root / path).is_file():
                raise ValueError(f"Invalid always-selected test: {entry}")
            if not any(Path(target) in path.parents for target in tests):
                tests.add(entry)
    return sorted(tests)


def select_ci_only(root: Path, paths: list[str], base: str, head: str) -> list[str]:
    # Revision mode, including next's kind omission, is the CI changes job's
    # selection. Keep workflow-hunk and version-only decisions in its owner.
    scratch = root / ".projects" / "preflight"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="selection-", dir=scratch) as directory:
        output = Path(directory) / "outputs"
        command = [
            sys.executable,
            "tools/e2e-ci-selection/select_tiers.py",
            "--registry",
            ".github/e2e-selection.yaml",
            "--base",
            f"origin/{base}",
            "--head",
            head,
        ]
        if base == "next":
            command.append("--omit-kind")
        result = subprocess.run(
            command,
            cwd=root,
            env={**os.environ, "GITHUB_OUTPUT": str(output)},
            text=True,
            capture_output=True,
            check=False,
        )
        if result.returncode:
            raise ValueError(f"CI tier selection failed: {result.stderr.strip()}")
        flags = dict(line.split("=", 1) for line in output.read_text().splitlines())
    selected = [
        tier
        for tier in ("skill", "local", "local-release", "cluster", "released-upgrade")
        if flags.get(tier.replace("-", "_")) == "true"
    ]
    if flags.get("cluster") == "true":
        selected.extend(("e2e-ladder-cluster", "e2e-cluster-chart-regressions"))
    if flags.get("approval_resume") == "true":
        selected.append("e2e-cluster-approval-resume-restarts")
    if flags.get("factory") == "true":
        selected.append("factory scenario on e2e-ladder-cluster")
    if flags.get("released_upgrade") == "true":
        selected.append("e2e-cluster-upgrade-matrix")
    if flags.get("released_upgrade_full") == "true":
        selected.extend(("e2e-released-upgrade", "e2e-released-upgrade-negative"))
    return selected


def run_process(
    command: list[str], root: Path, environment: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    # A signal must reap the owned foreground process tree before its Compose
    # services or committed checkout can be removed.
    try:
        process = subprocess.Popen(
            command,
            cwd=root,
            env=environment,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
    except OSError as error:
        raise TransientError("Cannot start the required preflight toolchain command") from error
    try:
        output, _ = process.communicate()
    except BaseException:
        try:
            os.killpg(process.pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        try:
            process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
        raise
    return subprocess.CompletedProcess(command, process.returncode, output)


def run_check(
    root: Path,
    name: str,
    command: list[str],
    environment: dict[str, str] | None = None,
) -> dict[str, str]:
    result = run_process(command, root, environment)
    return {
        "name": name,
        "status": "passed" if result.returncode == 0 else "failed",
        "detail": "\n".join(result.stdout.splitlines()[-30:])[-12000:] or shlex.join(command),
    }


def waiver_issues(body: str) -> list[int]:
    # Match the workflow's comment, fence, indented-code and inline-code rules.
    uncommented = re.sub(r"<!--.*?(?:-->|$)", "", body, flags=re.S)
    visible = []
    fence: tuple[str, int] | None = None
    for line in uncommented.splitlines():
        if fence:
            if re.fullmatch(r" {0,3}" + re.escape(fence[0]) + "{" + str(fence[1]) + r",}\s*", line):
                fence = None
            continue
        opening = re.match(r"^ {0,3}(`{3,}|~{3,})(.*)$", line)
        if opening:
            fence = (opening[1][0], len(opening[1]))
        elif not re.match(r"^(?: {4}| {0,3}\t)", line):
            visible.append(line)
    numbers: set[int] = set()
    for line in visible:
        for cell in re.split(r"(?<!\\)\|", line):
            text = re.sub(r"(`+).*?\1", "", cell)
            waiver = re.search(r"(?:^|\s)Discovery waiver:\s*(.+)$", text, flags=re.I)
            if waiver:
                references = re.sub(r"\[[^\]]*\]\([^)]*\)", "", waiver[1])
                numbers.update(
                    int(number) for number in re.findall(r"(?<![\w/])#([1-9][0-9]*)\b", references)
                )
    if len(numbers) > 100:
        raise ValueError("Too many waiver issue references")
    return sorted(numbers)


def open_waiver_issues(root: Path, repository: str, body: str) -> list[int]:
    numbers = waiver_issues(body)
    owner, name = repository.split("/", 1)
    opened = []
    for offset in range(0, len(numbers), 50):
        batch = numbers[offset : offset + 50]
        fields = "\n".join(
            f"issue{number}: issueOrPullRequest(number: {number}) "
            "{ __typename ... on Issue { number state } }"
            for number in batch
        )
        query = (
            "query($owner: String!, $repo: String!) { repository(owner: $owner, name: $repo) { "
            + fields
            + " } }"
        )
        result = gh_json(
            root, "graphql", "-f", f"query={query}", "-f", f"owner={owner}", "-f", f"repo={name}"
        )
        if result.get("errors"):
            raise ValueError("GitHub waiver issue lookup failed")
        records = result.get("data", {}).get("repository")
        if not isinstance(records, dict):
            raise ValueError("GitHub waiver issue repository lookup failed")
        for number in batch:
            key = f"issue{number}"
            if key not in records:
                raise ValueError("GitHub waiver issue lookup was incomplete")
            record = records[key]
            if (
                record
                and record.get("__typename") == "Issue"
                and record.get("state") == "OPEN"
                and record.get("number") == number
            ):
                opened.append(number)
    return opened


def fix_pin_needs_curie(root: Path, event: Path, directory: Path) -> bool:
    output = directory / "fix-pin-output"
    check = run_check(
        root,
        "Fix pin build selection",
        [sys.executable, "tools/fix-pin-ci/check.py", "--event", str(event), "--needs-curie"],
        {**os.environ, "GITHUB_OUTPUT": str(output)},
    )
    if check["status"] == "failed":
        raise ValueError(check["detail"])
    return "needed=true" in output.read_text().splitlines()


def check_pr_body(
    root: Path,
    body_file: Path,
    title: str,
    paths: list[str],
    base: str,
    head: str,
    repository: str,
    *,
    environment: dict[str, str] | None = None,
) -> list[dict[str, str]]:
    body = body_file.read_text()
    scratch = root / ".projects" / "preflight"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="body-", dir=scratch) as directory:
        temporary = Path(directory)
        inputs = {
            "body.md": body,
            "title.txt": title,
            "changed-files.json": json.dumps(paths),
            "open-issues.json": json.dumps(open_waiver_issues(root, repository, body)),
            "event.json": json.dumps(
                {
                    "repository": {"full_name": repository},
                    "pull_request": {
                        "body": body,
                        "title": title,
                        "base": {
                            "ref": base,
                            "sha": git(root, "rev-parse", f"origin/{base}"),
                            "repo": {"full_name": repository},
                        },
                        "head": {"sha": git(root, "rev-parse", head)},
                    },
                }
            ),
        }
        for name, content in inputs.items():
            destination = temporary / name
            destination.write_text(content)
            destination.chmod(0o600)
        checks = [
            run_check(
                root,
                "PR body",
                [
                    "bash",
                    "scripts/check-pr-body.sh",
                    str(temporary / "body.md"),
                    "--title-file",
                    str(temporary / "title.txt"),
                    "--changed-files-file",
                    str(temporary / "changed-files.json"),
                    "--open-issues-file",
                    str(temporary / "open-issues.json"),
                ],
                environment,
            )
        ]
        needed = fix_pin_needs_curie(root, temporary / "event.json", temporary)
        binary = root / "cli/target/release/curie"
        if needed:
            build = run_check(
                root,
                "Fix pin release build",
                [
                    "cargo",
                    "build",
                    "--release",
                    "--locked",
                    "--manifest-path",
                    "cli/Cargo.toml",
                ],
                environment,
            )
            checks.append(build)
            if build["status"] == "failed":
                return checks
            metadata = run_process(
                [
                    "cargo",
                    "metadata",
                    "--quiet",
                    "--no-deps",
                    "--format-version=1",
                    "--manifest-path",
                    "cli/Cargo.toml",
                ],
                root,
                environment,
            )
            if metadata.returncode:
                raise ValueError("Cannot locate the release binary through Cargo metadata")
            binary = Path(json.loads(metadata.stdout)["target_directory"]) / "release/curie"
        services = (
            private_services(root, root)
            if needed and environment is None
            else nullcontext(environment)
        )
        with services as service_environment:
            checks.append(
                run_check(
                    root,
                    "Fix pin",
                    [
                        sys.executable,
                        "tools/fix-pin-ci/check.py",
                        "--event",
                        str(temporary / "event.json"),
                        "--curie",
                        str(binary),
                        "--ref",
                        head,
                    ],
                    service_environment,
                )
            )
        return checks


class Interrupted(BaseException):
    def __init__(self, signum: int) -> None:
        self.signum = signum


def interrupt(signum: int, _frame: Any) -> Never:
    raise Interrupted(signum)


def docker_output(root: Path, environment: dict[str, str], *arguments: str) -> str:
    try:
        result = subprocess.run(
            ["docker", *arguments],
            cwd=root,
            env=environment,
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError as error:
        raise TransientError("Cannot run Docker for the private preflight services") from error
    if result.returncode:
        # Docker's diagnostics can contain substituted credentials. The exit
        # and operation suffice here; the caller can inspect its private stack.
        error_class = GateFailure if "config" in arguments else TransientError
        raise error_class(f"Docker {arguments[0]} operation failed (exit {result.returncode})")
    return result.stdout.strip()


def owned_resources(root: Path, environment: dict[str, str], project: str) -> list[str]:
    label = f"label=com.docker.compose.project={project}"
    return [
        identity
        for arguments in (("ps", "-aq"), ("network", "ls", "-q"), ("volume", "ls", "-q"))
        for identity in docker_output(root, environment, *arguments, "--filter", label).splitlines()
    ]


def validate_private_config(
    configuration: dict[str, Any],
    project: str,
    published: dict[str, list[int]],
) -> None:
    for service in published:
        if any(
            port.get("host_ip") != "127.0.0.1"
            for port in configuration["services"][service]["ports"]
        ):
            raise GateFailure("The private Compose configuration retained a shared binding")
    if configuration["networks"]["curie_runner"]["name"] != f"{project}_runner":
        raise GateFailure("The private runner network has an incorrect owner")


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def port_is_occupied(port: int) -> bool:
    with socket.socket() as listener:
        try:
            listener.bind(("127.0.0.1", port))
        except OSError:
            return True
    return False


@contextmanager
def private_services(root: Path, worktree: Path) -> Iterator[dict[str, str]]:
    """Own the Python shard's backing services and verify their teardown."""
    project = "curie-preflight-" + hashlib.sha256(str(worktree.resolve()).encode()).hexdigest()[:10]
    scratch = root / ".projects" / "preflight"
    scratch.mkdir(parents=True, exist_ok=True)
    lock_directory = worktree / ".projects" / "preflight"
    lock_directory.mkdir(parents=True, exist_ok=True)
    # The project identity is intentionally stable for this worktree. A lock
    # and label inventory prevent simultaneous invocations adopting each other.
    with (lock_directory / f"{project}.lock").open("w") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise TransientError(
                "Another full preflight owns this worktree's Compose project"
            ) from error
        with tempfile.TemporaryDirectory(prefix="compose-", dir=scratch) as directory:
            override = Path(directory) / "compose.yaml"
            environment = {
                **os.environ,
                "COMPOSE_PROJECT_NAME": project,
                "COMPOSE_FILE": f"{root / 'compose.dev.yaml'}:{override}",
                "CURIE_LOCAL_POSTGRES_PASSWORD": "postgres",
                "VALKEY_PASSWORD": "valkeypass",
                "TEST_VALKEY_PW": "valkeypass",
                "S3_ACCESS_KEY": "rustfs",
                "TEST_S3_ACCESS_KEY": "rustfs",
                "S3_SECRET_KEY": "rustfssecret",
                "TEST_S3_SECRET_KEY": "rustfssecret",
                "LANGFUSE_PUBLIC_KEY": "pk-lf-curie-dev",
                "TEST_LANGFUSE_PUBLIC_KEY": "pk-lf-curie-dev",
                "LANGFUSE_SECRET_KEY": "sk-lf-curie-dev",
                "TEST_LANGFUSE_SECRET_KEY": "sk-lf-curie-dev",
                "CI_REQUIRE_POSTGRES_TESTS": "1",
                "CI_REQUIRE_VALKEY_TESTS": "1",
                "CURIE_DOCKER_NETWORK": f"{project}_runner",
            }
            compose = [
                "compose",
                "-p",
                project,
                "-f",
                str(root / "compose.dev.yaml"),
                "-f",
                str(override),
            ]
            if owned_resources(root, environment, project):
                raise TransientError(
                    "The private preflight project already has resources; preserve its owner"
                )
            previous = {
                number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)
            }
            for number in previous:
                signal.signal(number, interrupt)

            def cleanup() -> None:
                for number in previous:
                    signal.signal(number, signal.SIG_IGN)
                try:
                    docker_output(root, environment, *compose, "--profile", "full", "down", "-v")
                    if owned_resources(root, environment, project):
                        raise TransientError(
                            "The owned preflight Compose resources were not removed"
                        )
                    print(f"preflight: removed private Compose project {project}", file=sys.stderr)
                finally:
                    for number in previous:
                        signal.signal(number, interrupt)

            try:
                # Docker allocates every backing port. Only the web readiness
                # URL must be known before the existing startup helper runs.
                for attempt in range(3):
                    web_port = free_port()
                    published = {
                        "postgres": [5432],
                        "valkey": [6379],
                        "clickhouse": [8123, 9000],
                        "rustfs": [9000, 9001],
                        "langfuse-web": [3000],
                        "otel-collector": [4317, 4318, 8888],
                    }
                    lines = ["services:"]
                    for service, ports in published.items():
                        bindings = [
                            f"127.0.0.1:{web_port if service == 'langfuse-web' else ''}:{port}"
                            for port in ports
                        ]
                        lines.extend(
                            (f"  {service}:", "    ports: !override " + json.dumps(bindings))
                        )
                    lines.extend(("networks:", "  curie_runner:", f"    name: {project}_runner"))
                    override.write_text("\n".join(lines) + "\n")
                    configuration = json.loads(
                        docker_output(
                            root,
                            environment,
                            *compose,
                            "--profile",
                            "full",
                            "config",
                            "--format",
                            "json",
                        )
                    )
                    validate_private_config(configuration, project, published)
                    environment["TEST_LANGFUSE_HOST"] = f"http://127.0.0.1:{web_port}"
                    environment["LANGFUSE_HOST"] = environment["TEST_LANGFUSE_HOST"]
                    startup = run_check(
                        root,
                        "Python backing services",
                        [
                            sys.executable,
                            "scripts/wait-for-langfuse.py",
                            "--start",
                            "--timeout-seconds",
                            "480",
                        ],
                        environment,
                    )
                    if startup["status"] == "passed":
                        break
                    web = docker_output(
                        root,
                        environment,
                        *compose,
                        "ps",
                        "--status",
                        "running",
                        "-q",
                        "langfuse-web",
                    )
                    collision = not web and port_is_occupied(web_port)
                    if not collision or attempt == 2:
                        raise TransientError(startup["detail"])
                    cleanup()
                print(f"preflight: started private Compose project {project}", file=sys.stderr)

                def port(service: str, container_port: int) -> int:
                    address = docker_output(
                        root, environment, *compose, "port", service, str(container_port)
                    )
                    host, value = address.rsplit(":", 1)
                    if host != "127.0.0.1":
                        raise GateFailure("The private Compose port is not bound to loopback")
                    return int(value)

                postgres = port("postgres", 5432)
                valkey = port("valkey", 6379)
                s3 = port("rustfs", 9000)
                otel = port("otel-collector", 4318)
                environment.update(
                    released_upgrade_environment(
                        environment["COMPOSE_PROJECT_NAME"], environment["COMPOSE_FILE"], postgres
                    )
                )
                environment.update(
                    {
                        "DATABASE_URL": f"postgresql+asyncpg://postgres:postgres@127.0.0.1:{postgres}/postgres",
                        "TEST_DATABASE_URL": f"postgresql+asyncpg://postgres:postgres@127.0.0.1:{postgres}/postgres",
                        "TEST_VALKEY_HOST": "127.0.0.1",
                        "TEST_VALKEY_PORT": str(valkey),
                        "VALKEY_HOST": "127.0.0.1",
                        "VALKEY_PORT": str(valkey),
                        "S3_ENDPOINT_URL": f"http://127.0.0.1:{s3}",
                        "TEST_S3_ENDPOINT_URL": f"http://127.0.0.1:{s3}",
                        "TEST_OTEL_COLLECTOR_ENDPOINT": f"http://127.0.0.1:{otel}/v1/traces",
                        "OTEL_EXPORTER_OTLP_ENDPOINT": f"http://127.0.0.1:{otel}",
                        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
                    }
                )
                migration = run_check(
                    root / "apps/api",
                    "Python test database migrations",
                    ["uv", "run", "alembic", "upgrade", "head"],
                    environment,
                )
                if migration["status"] == "failed":
                    raise GateFailure(migration["detail"])
                yield environment
            finally:
                try:
                    cleanup()
                finally:
                    for number, handler in previous.items():
                        signal.signal(number, handler)


def render_command(root: Path, check: Check, base: str, head: str) -> str:
    command = check.command
    range_ = f"origin/{base}..{head}"
    command = command.replace(
        '"${{ github.event.pull_request.base.sha }}..${{ github.event.pull_request.head.sha }}"',
        shlex.quote(range_),
    )
    if check.job == "gitleaks":
        workflow = load_workflow(root, "gitleaks.yaml")
        image = workflow["jobs"]["gitleaks"]["env"]["GITLEAKS_IMAGE"]
        command = command.replace('"${{ github.workspace }}:/repo"', shlex.quote(f"{root}:/repo"))
        command = command.replace('"$GITLEAKS_IMAGE"', shlex.quote(image))
        command = command.replace(
            '${GITLEAKS_LOG_OPTS:+"$GITLEAKS_LOG_OPTS"}', shlex.quote(f"--log-opts={range_}")
        )
        # A linked worktree's .git file points outside its checkout. Preserve
        # those paths inside the scanner container so Git reads the same refs.
        git_dir = Path(git(root, "rev-parse", "--absolute-git-dir"))
        common_dir = Path(git(root, "rev-parse", "--path-format=absolute", "--git-common-dir"))
        if git_dir != root / ".git":
            mounts = " ".join(
                f"-v {shlex.quote(f'{path}:{path}:ro')}" for path in sorted({git_dir, common_dir})
            )
            command = command.replace("docker run --rm", f"docker run --rm {mounts}", 1)
    return command


@contextmanager
def committed_checkout(root: Path, head: str) -> Iterator[Path]:
    """Run against the selected commit without changing the caller's files."""
    scratch = root / ".projects" / "preflight"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="run-", dir=scratch) as directory:
        checkout = Path(directory) / "checkout"
        registered = False
        try:
            git(root, "worktree", "add", "--quiet", "--detach", str(checkout), head)
            registered = True
            yield checkout
        finally:
            if registered:
                git(root, "worktree", "remove", "--force", str(checkout))
                if checkout.exists():
                    raise ValueError("The owned preflight worktree was not removed")


def execute_checks(root: Path, base: str, head: str, dry_run: bool, report: dict[str, Any]) -> None:
    validate_workflows(root)
    paths = changed_paths(root, base, head)
    for check in selected_checks(paths):
        command = render_command(root, check, base, head)
        label = f"{check.workflow}:{check.job}: {check.step}"
        entry = {
            "name": label,
            "status": "planned" if dry_run else "passed",
            "detail": command,
            "workflow": check.workflow,
            "job": check.job,
            "step": check.step,
            "command": command,
            "cwd": check.cwd,
            "group": check.group,
        }
        report["checks"].append(entry)
        if dry_run:
            continue
        print(f"preflight: {label}", file=sys.stderr, flush=True)
        if check.job == "ui" and check.step == "Lint":
            print(
                f"preflight: prerequisite {UI_SETUP.workflow}:{UI_SETUP.job}: {UI_SETUP.step}",
                file=sys.stderr,
                flush=True,
            )
            command = f"{UI_SETUP.command}\n{command}"
        try:
            result = run_process(["bash", "-e", "-o", "pipefail", "-c", command], root / check.cwd)
            exit_code, output = result.returncode, result.stdout
        except OSError as error:
            exit_code, output = 127, str(error)
        if exit_code:
            tail = "\n".join(output.splitlines()[-30:])[-12000:]
            entry.update(status="failed", detail=tail)
            report["failures"].append({"check": label, "exit_code": exit_code, "output_tail": tail})
            print(f"preflight: failed {label}\n{tail}", file=sys.stderr)
        else:
            print(f"preflight: passed {label}", file=sys.stderr)


def record_check(report: dict[str, Any], check: dict[str, str]) -> None:
    report["checks"].append(check)
    if check["status"] == "failed":
        report["failures"].append(
            {"check": check["name"], "exit_code": 1, "output_tail": check["detail"]}
        )
    print(f"preflight: {check['status']} {check['name']}", file=sys.stderr)


def body_needs_services(root: Path, body_file: Path) -> bool:
    scratch = root / ".projects" / "preflight"
    scratch.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="pin-", dir=scratch) as directory:
        event = Path(directory) / "event.json"
        event.write_text(json.dumps({"pull_request": {"body": body_file.read_text()}}))
        event.chmod(0o600)
        return fix_pin_needs_curie(root, event, Path(directory))


def execute_full(
    root: Path,
    worktree: Path,
    base: str,
    head: str,
    dry_run: bool,
    body_file: Path | None,
    title: str,
    repository: str,
    report: dict[str, Any],
) -> None:
    execute_checks(root, base, head, dry_run, report)
    # Disabling rename detection includes both old and new filenames, matching
    # the PR workflow's changed-file set and both affected ownership boundaries.
    paths = changed_paths(root, base, head)
    tests = select_python_tests(root, paths)
    report["ci_only"] = select_ci_only(root, paths, base, head)
    if body_file is None:
        for name in ("PR body", "Fix pin"):
            report["checks"].append(
                {
                    "name": name,
                    "status": "skipped",
                    "detail": "Supply --pr-body <file> and --title <text> "
                    "to check the proposed pull request",
                }
            )
    elif dry_run:
        for name, command in (
            ("PR body", "scripts/check-pr-body.sh"),
            ("Fix pin", "tools/fix-pin-ci/check.py"),
        ):
            report["checks"].append(
                {"name": name, "status": "planned", "detail": f"Run {command} against {body_file}"}
            )
    selection = {
        "name": "Affected Python tests",
        "status": "planned" if tests else "skipped",
        "detail": shlex.join(pytest_command(tests))
        if tests
        else "No changed workspace members select Python tests",
    }
    if dry_run:
        report["checks"].append(selection)
        return
    needs_services = bool(tests) or (body_file is not None and body_needs_services(root, body_file))
    services = private_services(root, worktree) if needs_services else nullcontext(None)
    with services as environment:
        if body_file is not None:
            for check in check_pr_body(
                root, body_file, title, paths, base, head, repository, environment=environment
            ):
                record_check(report, check)
        if tests:
            record_check(
                report,
                run_check(
                    root,
                    "Affected Python tests",
                    pytest_command(tests),
                    environment,
                ),
            )
        else:
            report["checks"].append(selection)


class Parser(argparse.ArgumentParser):
    def error(self, message: str) -> Never:
        raise ValueError(message)


def main(argv: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if argv is None else argv
    parser = Parser(description=__doc__)
    parser.add_argument("--fast", action="store_true", help="Run only the service-free PR gates")
    parser.add_argument("--base", default="main", help="Compare against origin/<base>")
    parser.add_argument("--head", default="HEAD", help="Commit at the end of the comparison")
    parser.add_argument(
        "--pr-body", type=Path, help="File containing the proposed pull request body"
    )
    parser.add_argument("--title", default="", help="Proposed pull request title")
    parser.add_argument(
        "--dry-run", action="store_true", help="Print checks without executing them"
    )
    parser.add_argument("--json", action="store_true", help="Emit one JSON result")
    json_output = "--json" in arguments
    report: dict[str, Any] = {
        "passed": False,
        "checks": [],
        "failures": [],
        "ci_only": [],
        "base": {"contains_tip": False, "failing_required_checks": []},
    }
    previous_handlers: dict[int, Any] = {}
    try:
        args = parser.parse_args(arguments)
        if args.base.startswith("-") or args.head.startswith("-"):
            raise ValueError("Base and head must be Git references, not options")
        report.update(head=args.head, dry_run=args.dry_run, tier="fast" if args.fast else "full")
        report["base"]["ref"] = args.base
        root = Path(git(Path.cwd(), "rev-parse", "--show-toplevel"))
        head = git(root, "rev-parse", "--verify", f"{args.head}^{{commit}}")
        body_file = args.pr_body.resolve() if args.pr_body else None
        if body_file is not None and not body_file.is_file():
            raise ValueError("The proposed pull request body file does not exist")
        previous_handlers = {
            number: signal.getsignal(number) for number in (signal.SIGINT, signal.SIGTERM)
        }
        for number in previous_handlers:
            signal.signal(number, interrupt)
        if args.fast:
            git(root, "rev-parse", "--verify", f"origin/{args.base}^{{commit}}")
            contains_tip = subprocess.run(
                ["git", "merge-base", "--is-ancestor", f"origin/{args.base}", head],
                cwd=root,
                check=False,
                capture_output=True,
            )
            if contains_tip.returncode not in (0, 1):
                raise ValueError("Cannot compare the fast tier's base tip to its selected head")
            report["base"]["contains_tip"] = contains_tip.returncode == 0
            if args.dry_run:
                execute_checks(root, args.base, args.head, True, report)
            else:
                with committed_checkout(root, head) as checkout:
                    execute_checks(checkout, args.base, "HEAD", False, report)
        else:
            freshness = check_base_freshness(root, args.base, head)
            report["base"]["contains_tip"] = freshness["contains_tip"]
            record_check(
                report,
                {
                    "name": "Base freshness",
                    "status": "passed" if freshness["contains_tip"] else "failed",
                    "detail": freshness["detail"]
                    + ("" if freshness["contains_tip"] else "; " + freshness["fix"]),
                },
            )
            if not freshness["contains_tip"]:
                report.update(error=freshness["detail"], fix=freshness["fix"])
            else:
                try:
                    repository = subprocess.run(
                        ["gh", "repo", "view", "--json", "nameWithOwner", "--jq", ".nameWithOwner"],
                        cwd=root,
                        text=True,
                        capture_output=True,
                        check=False,
                    )
                except OSError as error:
                    raise TransientError(
                        "Cannot run authenticated gh to read this repository"
                    ) from error
                if repository.returncode:
                    raise TransientError("Cannot read this repository through authenticated gh")
                if not re.fullmatch(r"[^/\s]+/[^/\s]+", repository.stdout.strip()):
                    raise ValueError("GitHub returned an invalid repository identity")
                repository_name = repository.stdout.strip()
                failing = base_health(root, args.base, repository_name)
                report["base"]["failing_required_checks"] = failing
                if failing:
                    print(
                        "preflight: warning: failing required checks on the base: "
                        + ", ".join(failing),
                        file=sys.stderr,
                    )
                if args.dry_run:
                    execute_full(
                        root,
                        root,
                        args.base,
                        args.head,
                        True,
                        body_file,
                        args.title,
                        repository_name,
                        report,
                    )
                else:
                    with committed_checkout(root, head) as checkout:
                        execute_full(
                            checkout,
                            root,
                            args.base,
                            "HEAD",
                            False,
                            body_file,
                            args.title,
                            repository_name,
                            report,
                        )
        report["passed"] = not report["failures"]
        if report["failures"] and "error" not in report:
            report.update(
                error=f"{report['tier'].capitalize()} preflight checks failed",
                fix="Fix every named check, then rerun curie dev preflight"
                + (" --fast" if args.fast else ""),
            )
        exit_code = 0 if report["passed"] else 1
    except TransientError as error:
        report.update(
            error=str(error),
            fix="Restore the named toolchain or service dependency, then rerun curie dev preflight",
        )
        exit_code = 3
    except GateFailure as error:
        report.update(
            error=str(error), fix="Fix the named verification gate, then rerun curie dev preflight"
        )
        exit_code = 1
    except (ValueError, OSError) as error:
        report.update(
            error=str(error),
            fix="Check the repository toolchain and fetch the requested "
            "origin base, then rerun curie dev preflight",
        )
        exit_code = 2
    except (Interrupted, KeyboardInterrupt) as error:
        report.update(
            error="Preflight interrupted", fix="Rerun curie dev preflight after the interruption"
        )
        exit_code = 128 + (error.signum if isinstance(error, Interrupted) else signal.SIGINT)
    finally:
        for number, handler in previous_handlers.items():
            signal.signal(number, handler)
    if json_output:
        print(json.dumps(report))
    elif "error" in report:
        print(f"preflight: {report['error']}\nFix: {report['fix']}", file=sys.stderr)
    elif report.get("dry_run"):
        for check in report["checks"]:
            print(f"{check['name']}: {check['status']}\n{check['detail']}")
    else:
        print(f"{report['tier'].capitalize()} preflight passed ({len(report['checks'])} checks)")
    if not json_output:
        for entry in report["ci_only"]:
            print(f"{entry}: runs in CI only")
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
