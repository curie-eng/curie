"""Proof harness: a repository's real toolchain installs and its real checks run
inside the pinned runner image under the product's actual isolation posture.

Every assertion here is on an observed exit status or observed output text from a
real ``docker run`` against the resolved runner image. There are **no mocks** by
design: a fake cannot prove anything about a read-only rootfs, a ``noexec``
tmpfs, or ``--network none``.

The regression class this whole module exists to catch is "the documented recipe
stopped working inside the sandbox and nobody noticed because the docs are
prose". If the sandbox posture, the image toolchain, or the recipe drift apart,
one of these tests goes red.

Gating rules (see the plan's Edge cases):

- no ``docker`` binary, or a binary with an unreachable daemon -> **skip**,
  unless ``CURIE_REPO_TOOLCHAIN_PROOF=required``, which **fails**;
- the resolved runner image absent locally -> **skip** with a message naming
  ``curie build``, unless required, which **fails**;
- hardening disabled (``run_args()`` empty) -> **fail**, because a vacuous proof
  is worse than no proof.

The Python job still does not build ``curie-runner``, so its collection of this
module skips every container leg. Merge gating lives in the dedicated
``repo-toolchain-proof`` CI job, which builds the runner image and runs this
module with ``CURIE_REPO_TOOLCHAIN_PROOF=required``. An absent image or an
absent Docker daemon then fails that job instead of skipping.

Evidence JSON is written to ``CURIE_PROOF_EVIDENCE_DIR`` when that is set, and
only otherwise to pytest's ``tmp_path`` (which pytest deletes, making it useless
as durable PR evidence). The resolved path is printed either way.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
import zipfile
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_REPO = Path(__file__).resolve().parent / "fixtures" / "repo_toolchain"
GUIDE = REPO_ROOT / "docs" / "guides" / "repository-toolchain-in-the-managed-sandbox.md"
PIP_CONF = REPO_ROOT / "runner" / "pip.conf"
DOCKERFILE = REPO_ROOT / "runner" / "Dockerfile"
REGISTRY_EGRESS_CHECK = REPO_ROOT / "scripts" / "check-registry-egress.py"

# --- product posture, imported rather than hardcoded -------------------------
#
# The harness must prove the recipe against the flags the Docker sandbox driver
# really applies. Importing them means a future drift in ``RunnerHardening``
# breaks this harness instead of silently diverging from it. The literal
# fallback exists only for a collection root that cannot see ``curie_worker``;
# ``test_isolation_flags_match_the_product_posture`` asserts the two agree.

_LITERAL_RUN_ARGS = [
    "--read-only",
    "--tmpfs",
    "/tmp:rw,mode=1777",
    "--tmpfs",
    "/home/runner:rw,mode=1777",
    "--cap-drop",
    "ALL",
    "--security-opt",
    "no-new-privileges",
    "--pids-limit",
    "512",
    "--memory",
    "768m",
    "--cpus",
    "1",
]
_LITERAL_WORKSPACE_MOUNT_PATH = "/workspace"

try:  # pragma: no cover - exercised by whichever branch the env provides
    from curie_worker.sandbox.docker import RunnerHardening

    HARDENING_ARGS = RunnerHardening().run_args()
    HARDENING_IMPORTED = True
except Exception:  # pragma: no cover
    RunnerHardening = None  # type: ignore[assignment]
    HARDENING_ARGS = list(_LITERAL_RUN_ARGS)
    HARDENING_IMPORTED = False

try:  # pragma: no cover
    from curie_worker.workspace import WORKSPACE_MOUNT_PATH
except Exception:  # pragma: no cover
    WORKSPACE_MOUNT_PATH = _LITERAL_WORKSPACE_MOUNT_PATH

# Same env var and same default as ``apps/worker/src/curie_worker/run.py``, so a
# contributor and CI prove the recipe against the image the worker would boot.
RUNNER_IMAGE = os.environ.get("CURIE_RUNNER_IMAGE", "curie-runner")

VENV = f"{WORKSPACE_MOUNT_PATH}/.venv"
WHEELHOUSE = f"{WORKSPACE_MOUNT_PATH}/.wheels"
WHEEL_BUILDER = f"{WORKSPACE_MOUNT_PATH}/tools/build_wheel.py"
PACKAGE_SRC = f"{WORKSPACE_MOUNT_PATH}/src"

# The fixture's own first-party dependency, shipped as SOURCE and built into a
# wheel at test time. No third-party ``.whl`` is committed: this repository
# commits zero wheels today, and vendoring a PyPI binary would be a
# supply-chain smell. The builder is stdlib-only (zipfile + hashlib) because the
# runner image carries NO setuptools anywhere -- an offline
# ``pip install <source tree>`` dies with
# ``BackendUnavailable: Cannot import 'setuptools.build_meta'``.
FIXTURE_DEP = "ratekit"
FIXTURE_DEP_PIN = "ratekit==1.2.0"

# Profile B's genuine third-party pin, installed from real PyPI. Small, pure
# Python, and widely mirrored, so a live leg failure means the registry path is
# broken rather than that the package is exotic.
LIVE_PIN = "packaging==25.0"

# The fixture repository's own documented check command, as its README states
# it. ``unittest`` is stdlib, which is exactly what keeps the offline closure to
# one tiny first-party wheel. The coder is instructed
# (``_PUBLISH_DESCRIPTION``) to find and run the repository's *documented*
# command; the harness runs exactly that string, so a green here means the
# documented command is the one that was proved.
CHECK_COMMAND = f"{VENV}/bin/python -m unittest discover -s tests -t . -v"

# Profile A, verbatim: build the wheel, make the venv, install offline.
INSTALL_VENDORED = (
    f"python {WHEEL_BUILDER} {PACKAGE_SRC} {WHEELHOUSE} && "
    f"python -m venv {VENV} && "
    f"{VENV}/bin/pip install --disable-pip-version-check "
    f"--no-index --find-links {WHEELHOUSE} {FIXTURE_DEP_PIN}"
)
# Profile B, verbatim: the same venv, a live registry.
INSTALL_LIVE = (
    f"python -m venv {VENV} && {VENV}/bin/pip install --disable-pip-version-check {LIVE_PIN}"
)

# Unique per test PROCESS (not per container): a sibling run of this module in
# another worktree gets its own prefix, so its containers can never appear in
# this process's cleanup listing and flake it.
CONTAINER_PREFIX = f"curie-2571-proof-{uuid.uuid4().hex[:8]}-"
DEFAULT_TIMEOUT = 300

# Every container name this process has actually launched via
# ``_run_in_sandbox``, across the whole module. The cleanup test asserts
# against this list rather than proving cleanliness for one synthetic
# container.
_LAUNCHED_CONTAINER_NAMES: list[str] = []

# The one seeded defect the whole red -> green -> red cycle exists to observe.
# ``FIX.patch`` turns ``used_in_window <= limit`` into ``<``; the fixture test
# that pins the boundary is the one -- and the only one -- that must fail before
# the patch and after its revert.
SEEDED_FAILING_TEST = "test_refuses_the_request_that_would_exceed_the_limit"

# Set to "required" by a pipeline that intends this module to gate: an absent
# docker or an absent runner image then FAILS instead of skipping.
PROOF_MODE_ENV = "CURIE_REPO_TOOLCHAIN_PROOF"
# Where durable evidence goes. pytest deletes ``tmp_path``, so a PR that wants
# to attach the recorded argv/exit statuses must point this somewhere kept.
EVIDENCE_DIR_ENV = "CURIE_PROOF_EVIDENCE_DIR"


# --- gates -------------------------------------------------------------------


def _proof_is_required() -> bool:
    return os.environ.get(PROOF_MODE_ENV, "").strip().casefold() == "required"


def _unavailable(reason: str) -> None:
    """Skip by default; fail loudly when the proof was declared required.

    Catches "the merge gate went green because the whole proof skipped". A
    silent skip is the correct default for a contributor with no runner image,
    but a pipeline that sets ``CURIE_REPO_TOOLCHAIN_PROOF=required`` has
    declared that a skip is a failure, and must not be able to degrade into one.
    """

    if _proof_is_required():
        raise AssertionError(
            f"{PROOF_MODE_ENV}=required, so this proof may not skip: {reason}. "
            "Build the runner image with `curie build` (or set CURIE_RUNNER_IMAGE) "
            "and make a Docker daemon reachable, or unset the variable to allow "
            "skipping."
        )
    pytest.skip(reason)


def _require_docker() -> None:
    if shutil.which("docker") is None:
        _unavailable("Docker is unavailable: docker CLI is not installed")
        return
    probe = subprocess.run(
        ["docker", "info", "--format", "{{.ServerVersion}}"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if probe.returncode != 0:
        reason = probe.stderr.strip() or probe.stdout.strip()
        _unavailable(f"Docker is unavailable: {reason}")


def _require_runner_image() -> None:
    _require_docker()
    probe = subprocess.run(
        ["docker", "image", "inspect", RUNNER_IMAGE],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if probe.returncode != 0:
        _unavailable(
            f"runner image {RUNNER_IMAGE!r} is not present locally; "
            "build it with `curie build` (or set CURIE_RUNNER_IMAGE)"
        )


def _require_non_vacuous_hardening() -> None:
    assert HARDENING_ARGS, (
        "the isolation posture is empty, so any result would be vacuous; "
        "RunnerHardening must be enabled for this proof to mean anything"
    )
    assert "--read-only" in HARDENING_ARGS, (
        "the read-only rootfs is the constraint under proof; without it a green "
        "install proves nothing about the managed sandbox"
    )


def _require_fixture() -> None:
    assert FIXTURE_REPO.is_dir(), (
        f"the fixture repository {FIXTURE_REPO} does not exist; it is the stand-in "
        "for a foreign repository the recipe must work on"
    )


def _evidence_dir(tmp_path: Path) -> Path:
    """Resolve the durable evidence directory, falling back to ``tmp_path``.

    Catches "the PR evidence was deleted by the test runner": pytest removes
    ``tmp_path``, so the JSON is only durable when a caller names a directory.
    """

    configured = os.environ.get(EVIDENCE_DIR_ENV, "").strip()
    if not configured:
        return tmp_path
    directory = Path(configured).expanduser()
    directory.mkdir(parents=True, exist_ok=True)
    return directory


def test_required_mode_fails_when_the_runner_image_is_absent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Catches required mode degrading to a skip when the image is missing.

    The dedicated CI job sets ``CURIE_REPO_TOOLCHAIN_PROOF=required`` so an
    absent ``curie-runner`` cannot silently skip. If ``_unavailable`` starts
    skipping again under that setting, this test goes red even while the
    workflow YAML still says required.
    """

    monkeypatch.setenv(PROOF_MODE_ENV, "required")
    monkeypatch.setattr(sys.modules[__name__], "RUNNER_IMAGE", "curie-runner-absent-2611")
    with pytest.raises(AssertionError, match="may not skip"):
        _require_runner_image()


def test_without_required_a_missing_image_skips(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The contributor default: no image, no required flag, skip rather than fail.

    The CI pin is what forbids this path on the merge gate. This test keeps
    the skip path honest so a future change cannot make a missing image fail
    for every local contributor.
    """

    monkeypatch.delenv(PROOF_MODE_ENV, raising=False)
    monkeypatch.setattr(sys.modules[__name__], "RUNNER_IMAGE", "curie-runner-absent-2611")
    with pytest.raises(pytest.skip.Exception, match="unavailable|not present locally"):
        _require_runner_image()


# --- pinning the unittest run itself ----------------------------------------


def _fixture_test_count() -> int:
    """Count the test methods the fixture's own suite ships.

    Read from the committed fixture rather than hardcoded, so deleting or adding
    a fixture test changes the expected ``Ran N tests`` instead of silently
    passing the cycle with a smaller suite.
    """

    total = 0
    for path in sorted((FIXTURE_REPO / "tests").rglob("test_*.py")):
        total += path.read_text().count("    def test_")
    return total


def _ran_count(step: Step) -> int:
    """Extract ``Ran N tests`` from a unittest run, or fail saying it is absent."""

    match = re.search(r"^Ran (\d+) tests?\b", step.output, flags=re.MULTILINE)
    assert match is not None, (
        f"the {step.leg!r} leg produced no unittest 'Ran N tests' summary, so no "
        f"test suite demonstrably executed at all:\n{step.output}"
    )
    return int(match.group(1))


def _assert_unittest_failed_on_the_seeded_defect(step: Step) -> None:
    """Assert this leg failed, and failed *because of the seeded defect*.

    Catches a fix (or a mutation of this harness's fixture) that deletes or
    empties the boundary test: an emptied suite still exits non-zero for some
    other reason, or exits zero, and either way stops naming
    ``SEEDED_FAILING_TEST`` in a ``FAIL:`` line.
    """

    assert step.exit_status != 0, (
        f"the {step.leg!r} leg must FAIL while the off-by-one is present:\n{step.output}"
    )
    named = re.search(
        rf"^FAIL: {re.escape(SEEDED_FAILING_TEST)}\b", step.output, flags=re.MULTILINE
    )
    assert named, (
        f"the failure must be {SEEDED_FAILING_TEST!r} itself; any other non-zero "
        f"exit means this leg is not observing the seeded off-by-one:\n{step.output}"
    )
    assert re.search(r"^FAILED \(failures=1\)", step.output, flags=re.MULTILINE), (
        f"exactly one unittest failure is expected on the seeded defect; a "
        f"different tally means the fixture drifted:\n{step.output}"
    )


def _assert_unittest_passed(step: Step) -> None:
    """Assert this leg ran the suite to a genuine ``OK``, not an empty green."""

    assert step.exit_status == 0, (
        f"the {step.leg!r} leg must PASS once the defect is fixed:\n{step.output}"
    )
    assert re.search(r"^OK\b", step.output, flags=re.MULTILINE), (
        f"a zero exit without unittest's OK summary is not a proved pass:\n{step.output}"
    )
    assert "FAIL:" not in step.output, f"a passing leg must report no failures:\n{step.output}"


# --- container plumbing ------------------------------------------------------


@dataclass
class Step:
    """One observed container invocation, recorded verbatim for PR evidence."""

    leg: str
    argv: list[str]
    exit_status: int
    duration_seconds: float
    timeout_seconds: int
    stdout: str
    stderr: str
    commit_sha: str | None = None

    @property
    def output(self) -> str:
        return f"{self.stdout}\n{self.stderr}"


@dataclass
class Evidence:
    steps: list[Step] = field(default_factory=list)

    def record(self, step: Step) -> Step:
        self.steps.append(step)
        return step

    def write(self, path: Path) -> Path:
        path.write_text(json.dumps({"steps": [asdict(s) for s in self.steps]}, indent=2))
        print(f"\n[2571] container evidence written to: {path}")
        return path


def _run_in_sandbox(
    leg: str,
    script: str,
    workspace: Path | None = None,
    network: str | None = "none",
    timeout: int = DEFAULT_TIMEOUT,
    workdir: str | None = None,
    extra_run_args: list[str] | None = None,
) -> Step:
    """Run one shell script inside the runner image under the product posture.

    ``--rm`` is unconditional: owned-resource cleanup is itself an AC.
    """

    name = f"{CONTAINER_PREFIX}{uuid.uuid4().hex[:12]}"
    _LAUNCHED_CONTAINER_NAMES.append(name)
    argv = ["docker", "run", "--rm", "--name", name, *HARDENING_ARGS]
    if extra_run_args:
        argv += extra_run_args
    if network is not None:
        argv += ["--network", network]
    if workspace is not None:
        argv += ["-v", f"{workspace}:{WORKSPACE_MOUNT_PATH}:rw"]
    argv += ["-w", workdir or (WORKSPACE_MOUNT_PATH if workspace else "/tmp")]
    # HOME is a writable tmpfs; pip's cache and venv bootstrap need one.
    argv += ["-e", "HOME=/home/runner", "--entrypoint", "sh", RUNNER_IMAGE, "-c", script]

    started = time.monotonic()
    try:
        completed = subprocess.run(
            argv, capture_output=True, text=True, timeout=timeout, check=False
        )
    except subprocess.TimeoutExpired as exc:  # bounded failure is an AC
        subprocess.run(["docker", "rm", "-f", name], capture_output=True, check=False)
        raise AssertionError(
            f"step {leg!r} did not terminate within {timeout}s; a hung step is "
            "not a bounded truthful failure"
        ) from exc
    return Step(
        leg=leg,
        argv=argv,
        exit_status=completed.returncode,
        duration_seconds=round(time.monotonic() - started, 3),
        timeout_seconds=timeout,
        stdout=completed.stdout,
        stderr=completed.stderr,
    )


def _host_git(workspace: Path, *args: str) -> str:
    completed = subprocess.run(
        [
            "git",
            "-c",
            "user.email=proof@curie.invalid",
            "-c",
            "user.name=Curie Proof Harness",
            "-C",
            str(workspace),
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, (
        f"git {' '.join(args)} failed: {completed.stderr or completed.stdout}"
    )
    return completed.stdout.strip()


def _materialize_fixture_clone(root: Path) -> Path:
    """Copy the fixture tree, make it container-writable, and git-init it.

    The committed fixture carries no ``.git`` (a nested repository confuses
    tooling and blame), and the copy means the harness never mutates the
    committed tree.
    """

    workspace = root / "repo"
    shutil.copytree(FIXTURE_REPO, workspace)
    # The runner image runs as uid 1000; a host tmpdir owned by another uid
    # would fail venv creation for a reason unrelated to the recipe.
    for path in [workspace, *workspace.rglob("*")]:
        os.chmod(path, 0o777 if path.is_dir() else 0o666)
    _host_git(workspace, "init", "-q")
    _host_git(workspace, "add", "-A")
    _host_git(workspace, "commit", "-q", "-m", "Import the fixture repository with its defect")
    return workspace


def _force_rmtree(path: Path) -> None:
    """Delete a tree that may contain files owned by the runner image uid.

    The proof containers run as uid 1000. GitHub-hosted runners are uid 1001,
    so a host ``rmtree`` hits ``PermissionError`` on the ``.venv`` the
    container created: tempfile tries to chmod before unlink. Delete from a
    root container instead, mounting the parent so only this path is removed.
    """

    path = path.resolve()
    if not path.exists():
        return
    completed = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "0",
            "--network",
            "none",
            "--entrypoint",
            "rm",
            "-v",
            f"{path.parent}:{path.parent}",
            RUNNER_IMAGE,
            "-rf",
            str(path),
        ],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
    if path.exists():
        raise AssertionError(
            f"could not delete {path} (exit {completed.returncode}): "
            f"{completed.stdout}\n{completed.stderr}"
        )


@contextmanager
def _owned_tempdir(prefix: str):
    """A ``TemporaryDirectory`` whose container-owned contents still delete."""

    tmp = tempfile.TemporaryDirectory(prefix=prefix, ignore_cleanup_errors=True)
    try:
        yield tmp.name
    finally:
        _force_rmtree(Path(tmp.name))
        tmp.cleanup()


# --- 1. the red -> green -> red cycle ---------------------------------------


def test_seeded_defect_fails_then_fix_passes_then_revert_fails_again(tmp_path: Path) -> None:
    """Catches a "check command" that is not actually checking anything, and a
    ``FIX.patch`` that deletes the failing test instead of fixing the defect.

    A single red or a single green cannot distinguish a real test command from
    one that always fails (or always passes). Running the *same* command string
    across defect -> fix -> revert, and requiring the exit statuses to move
    non-zero -> zero -> non-zero, is what makes the claim falsifiable. The
    per-leg commit SHA is recorded so the PR evidence names the exact tree each
    status belongs to.

    Exit status alone is not enough: deleting or emptying
    ``SEEDED_FAILING_TEST`` would reproduce the same non-zero -> zero -> non-zero
    signature. So the red legs must name that exact test in a ``FAIL:`` line with
    ``failures=1``, the green leg must reach unittest's ``OK``, and all three legs
    must report the same ``Ran N tests`` -- N being the count of test methods the
    committed fixture actually ships. A suite that shrinks, grows, or stops
    running now breaks this test.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()
    _require_fixture()

    evidence = Evidence()
    with _owned_tempdir(prefix="curie-2571-cycle-") as root:
        workspace = _materialize_fixture_clone(Path(root))

        install = evidence.record(
            _run_in_sandbox("install-vendored", INSTALL_VENDORED, workspace=workspace)
        )
        assert install.exit_status == 0, (
            f"the documented install must succeed before any check leg is meaningful:\n"
            f"{install.output}"
        )

        defect_sha = _host_git(workspace, "rev-parse", "HEAD")
        red = evidence.record(
            _run_in_sandbox("check-on-defect", CHECK_COMMAND, workspace=workspace)
        )
        red.commit_sha = defect_sha
        _assert_unittest_failed_on_the_seeded_defect(red)

        patch = workspace / "FIX.patch"
        assert patch.is_file(), f"the fixture must ship the one-line fix at {patch}"
        _host_git(workspace, "apply", "FIX.patch")
        _host_git(workspace, "commit", "-q", "-a", "-m", "Apply the one-line fix")
        fix_sha = _host_git(workspace, "rev-parse", "HEAD")

        green = evidence.record(_run_in_sandbox("check-on-fix", CHECK_COMMAND, workspace=workspace))
        green.commit_sha = fix_sha
        _assert_unittest_passed(green)

        _host_git(workspace, "revert", "--no-edit", "-n", fix_sha)
        _host_git(workspace, "commit", "-q", "-a", "-m", "Revert the one-line fix")
        revert_sha = _host_git(workspace, "rev-parse", "HEAD")

        red_again = evidence.record(
            _run_in_sandbox("check-on-revert", CHECK_COMMAND, workspace=workspace)
        )
        red_again.commit_sha = revert_sha
        _assert_unittest_failed_on_the_seeded_defect(red_again)

        expected = _fixture_test_count()
        assert expected >= 4, (
            "the fixture must ship a real suite around the seeded defect; a "
            f"suite of {expected} tests is too small to have been read correctly"
        )
        ran = {leg.leg: _ran_count(leg) for leg in (red, green, red_again)}
        assert set(ran.values()) == {expected}, (
            "all three legs must run the SAME set of tests, and it must be the "
            f"whole committed fixture suite ({expected} tests); observed {ran}. "
            "A leg that ran fewer tests means the fix deleted or skipped the "
            "failing test rather than fixing the defect"
        )

        assert defect_sha != fix_sha != revert_sha, (
            "each leg must have a distinct commit identity so the recorded exit "
            "statuses are attributable to specific trees"
        )

    evidence.write(_evidence_dir(tmp_path) / "repo-toolchain-cycle-evidence.json")


# --- 2. install under a read-only rootfs, with its control -------------------


def test_dependency_install_succeeds_under_read_only_rootfs(tmp_path: Path) -> None:
    """Catches "the install only worked because hardening was off".

    The positive leg proves the venv-in-/workspace install completes. The
    control leg proves, in the very same posture, that writing the image's
    ``site-packages`` is refused with a read-only-filesystem error. Without the
    control, a green install cannot be distinguished from an unhardened run.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()
    _require_fixture()

    evidence = Evidence()
    with _owned_tempdir(prefix="curie-2571-install-") as root:
        workspace = _materialize_fixture_clone(Path(root))

        install = evidence.record(
            _run_in_sandbox("install-vendored", INSTALL_VENDORED, workspace=workspace)
        )
        assert install.exit_status == 0, (
            f"the documented install must succeed with no operator exec fix-up:\n{install.output}"
        )

        control = evidence.record(
            _run_in_sandbox(
                "control-write-usr-local-lib",
                "touch /usr/local/lib/curie-2571-control",
                workspace=workspace,
            )
        )
        assert control.exit_status != 0, "the read-only rootfs must refuse a site-packages write"
        assert "read-only file system" in control.output.casefold(), (
            f"the refusal must be the read-only-rootfs one, not some other error:\n{control.output}"
        )

    evidence.write(_evidence_dir(tmp_path) / "repo-toolchain-install-evidence.json")


# --- 3. posture pinning ------------------------------------------------------


def test_isolation_flags_match_the_product_posture() -> None:
    """Catches the harness drifting away from the driver it claims to mirror.

    If ``RunnerHardening.run_args`` changes and this harness keeps proving the
    old flags, every other test in this module becomes a proof about a sandbox
    that no longer exists.
    """

    assert HARDENING_ARGS == _LITERAL_RUN_ARGS, (
        "the harness argv and the recorded product posture have diverged; update "
        "the literal alongside RunnerHardening.run_args"
    )
    assert WORKSPACE_MOUNT_PATH == _LITERAL_WORKSPACE_MOUNT_PATH
    if HARDENING_IMPORTED:
        assert RunnerHardening is not None
        assert HARDENING_ARGS == RunnerHardening().run_args()
    _require_non_vacuous_hardening()
    assert "--tmpfs" in HARDENING_ARGS, "the writable-scratch tmpfs mounts are part of the posture"
    assert f"{WORKSPACE_MOUNT_PATH}:rw,mode=1777" not in HARDENING_ARGS, (
        "/workspace is a bind mount, not a tmpfs; a tmpfs entry would shadow the repository"
    )


# --- 3b. the noexec divergence that is the whole reason for the recipe -------


def test_venv_console_scripts_run_from_workspace_but_not_from_tmpfs(tmp_path: Path) -> None:
    """Catches someone "simplifying" the recipe to put the venv in /tmp.

    The writable scratch mounts are ``--tmpfs <path>:rw,mode=1777``, and Docker's
    tmpfs default includes ``noexec``. So a venv created under ``/tmp`` (or
    ``/home/runner``) has console scripts -- ``bin/pip``, ``bin/pytest`` -- that
    die with ``Permission denied``, while its ``bin/python`` still works because
    that is a symlink onto the exec-able rootfs. The identical venv created under
    the bind-mounted ``/workspace`` works completely.

    This single divergence is why the documented recipe says "venv in
    /workspace", and why no operator ``exec`` fix-up is needed. If it ever stops
    being true, the guide's central instruction is stale.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()

    evidence = Evidence()
    with _owned_tempdir(prefix="curie-2571-noexec-") as root:
        workspace = Path(root) / "ws"
        workspace.mkdir()
        os.chmod(workspace, 0o777)

        tmpfs_leg = evidence.record(
            _run_in_sandbox(
                "console-script-on-tmpfs",
                "python -m venv /tmp/v && /tmp/v/bin/pip --version",
                workspace=workspace,
                workdir="/tmp",
            )
        )
        assert tmpfs_leg.exit_status != 0, (
            "a console script on the noexec tmpfs must fail; if it now succeeds, "
            "the tmpfs posture changed and the guide must be revisited:\n"
            f"{tmpfs_leg.output}"
        )
        assert "permission denied" in tmpfs_leg.output.casefold(), (
            f"the failure must be the noexec Permission denied one:\n{tmpfs_leg.output}"
        )

        tmpfs_python = evidence.record(
            _run_in_sandbox(
                "venv-python-on-tmpfs",
                "python -m venv /tmp/v && /tmp/v/bin/python -c 'print(1)'",
                workspace=workspace,
                workdir="/tmp",
            )
        )
        assert tmpfs_python.exit_status == 0, (
            "the venv's bin/python is a symlink onto the exec-able rootfs and must "
            f"still work even on the noexec tmpfs:\n{tmpfs_python.output}"
        )

        workspace_leg = evidence.record(
            _run_in_sandbox(
                "console-script-on-workspace",
                f"python -m venv {VENV} && {VENV}/bin/pip --version",
                workspace=workspace,
            )
        )
        assert workspace_leg.exit_status == 0, (
            "the same venv under the /workspace bind mount must work completely, "
            f"console scripts included:\n{workspace_leg.output}"
        )

    evidence.write(_evidence_dir(tmp_path) / "repo-toolchain-noexec-evidence.json")


# --- 4/5. bounded truthful negative controls ---------------------------------


def test_unreachable_registry_fails_bounded_and_truthfully(tmp_path: Path) -> None:
    """Catches a silent or unbounded failure when the registry is unreachable.

    An install that hangs forever, or that reports success without installing,
    is the failure mode an operator cannot debug. The assertion is on all three
    properties at once: non-zero exit, truthful text naming the unsatisfied
    requirement, and termination well inside the timeout.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()
    _require_fixture()

    evidence = Evidence()
    with _owned_tempdir(prefix="curie-2571-registry-") as root:
        workspace = _materialize_fixture_clone(Path(root))
        step = evidence.record(
            _run_in_sandbox(
                "install-against-unreachable-registry",
                f"python -m venv {VENV} && {VENV}/bin/pip install "
                f"--disable-pip-version-check --index-url https://192.0.2.1/simple {LIVE_PIN}",
                workspace=workspace,
                network="none",
                timeout=180,
            )
        )

    assert step.exit_status != 0, f"an unreachable registry must not report success:\n{step.output}"
    assert step.duration_seconds < step.timeout_seconds, "the failure must be bounded, not hung"
    lowered = step.output.casefold()
    assert (
        "could not find a version that satisfies the requirement" in lowered
        or "no matching distribution found" in lowered
    ), f"the failure text must truthfully name the unsatisfied requirement:\n{step.output}"
    assert "successfully installed" not in lowered, "a failed install must never claim success"

    evidence.write(_evidence_dir(tmp_path) / "repo-toolchain-unreachable-registry-evidence.json")


def test_default_index_blocked_egress_fails_on_the_image_retry_budget(
    tmp_path: Path,
) -> None:
    """Catches a fail-closed live-registry install hanging on pip's default retries.

    The cluster observation (#2614) was a default-index pip install under
    NetworkPolicy ENETUNREACH that ran for ~368s because pip retried every
    resolved address. The image ships ``/etc/pip.conf`` with ``retries = 0``
    so that command fails on the first attempt.

    Local analog: pin ``pypi.org`` to TEST-NET-1 so DNS "succeeds" and the TCP
    connect sits on pip's 15s socket timeout instead of failing at name
    resolution (``--network none``). Without the image pip.conf this takes
    ~90s (5 attempts * 15s) and trips the 45s bound below.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()
    _require_fixture()

    evidence = Evidence()
    with _owned_tempdir(prefix="curie-2614-blocked-") as root:
        workspace = _materialize_fixture_clone(Path(root))
        step = evidence.record(
            _run_in_sandbox(
                "install-default-index-blocked-egress",
                INSTALL_LIVE,
                workspace=workspace,
                network=None,
                timeout=45,
                extra_run_args=[
                    "--add-host",
                    "pypi.org:192.0.2.1",
                    "--add-host",
                    "files.pythonhosted.org:192.0.2.1",
                ],
            )
        )

    assert step.exit_status != 0, (
        f"a blocked default-index install must not report success:\n{step.output}"
    )
    assert step.duration_seconds < 30, (
        f"the image pip.conf must fail the blocked install on the first attempt, "
        f"not pip's default retry budget; observed {step.duration_seconds}s:\n"
        f"{step.output}"
    )
    lowered = step.output.casefold()
    assert (
        "could not find a version that satisfies the requirement" in lowered
        or "no matching distribution found" in lowered
        or "network is unreachable" in lowered
        or "timed out" in lowered
        or "connection" in lowered
    ), f"the failure text must truthfully name the blocked registry:\n{step.output}"
    assert "successfully installed" not in lowered, "a failed install must never claim success"

    evidence.write(_evidence_dir(tmp_path) / "repo-toolchain-blocked-default-index-evidence.json")


def test_missing_toolchain_fails_bounded_and_truthfully(tmp_path: Path) -> None:
    """Catches a missing toolchain being swallowed into a silent pass.

    If the repository documents a command whose toolchain the image does not
    carry, the coder must see a truthful "not found" and a non-zero status, so
    it reports the failure instead of publishing unverified changes.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()
    _require_fixture()

    evidence = Evidence()
    with _owned_tempdir(prefix="curie-2571-toolchain-") as root:
        workspace = _materialize_fixture_clone(Path(root))
        step = evidence.record(
            _run_in_sandbox(
                "missing-toolchain",
                "poetry install",
                workspace=workspace,
                network="none",
                timeout=120,
            )
        )

    assert step.exit_status != 0, f"an absent toolchain must not report success:\n{step.output}"
    assert step.duration_seconds < step.timeout_seconds, "the failure must be bounded, not hung"
    assert "not found" in step.output.casefold(), (
        f"the failure must truthfully name the missing command:\n{step.output}"
    )

    evidence.write(_evidence_dir(tmp_path) / "repo-toolchain-missing-toolchain-evidence.json")


# --- 6. Profile A: the "only" in "only the egress needed" --------------------


def test_vendored_profile_needs_no_registry_egress(tmp_path: Path) -> None:
    """Catches Profile A quietly acquiring a dependency on registry egress.

    AC bullet 3 asks for *only* the package-registry egress the repository
    needs. Profile A's answer is "none", and ``--network none`` is what proves
    it by construction: the fixture's dependency is built from committed source
    by a stdlib-only wheel builder and installed with
    ``--no-index --find-links``. If the builder, the source tree, or the offline
    install path breaks, this goes red -- which is the wanted signal, not a skip.

    The offline check leg pins the *named* seeded failure and the full
    ``Ran N tests`` count, because a bare non-zero exit here is also what a
    broken install, an import error, or an emptied suite would produce.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()
    _require_fixture()

    assert (FIXTURE_REPO / "tools" / "build_wheel.py").is_file(), (
        "Profile A requires the fixture's stdlib-only wheel builder at "
        "fixtures/repo_toolchain/tools/build_wheel.py; without it the zero-egress "
        "claim is undemonstrated"
    )

    evidence = Evidence()
    with _owned_tempdir(prefix="curie-2571-vendored-") as root:
        workspace = _materialize_fixture_clone(Path(root))

        install = evidence.record(
            _run_in_sandbox(
                "install-vendored-no-network",
                INSTALL_VENDORED,
                workspace=workspace,
                network="none",
            )
        )
        assert install.exit_status == 0, (
            f"the vendored install must succeed with no network at all:\n{install.output}"
        )
        assert "no matching distribution" not in install.output.casefold()

        importable = evidence.record(
            _run_in_sandbox(
                "import-vendored-dependency",
                f"{VENV}/bin/python -c 'import {FIXTURE_DEP}; print({FIXTURE_DEP}.__name__)'",
                workspace=workspace,
                network="none",
            )
        )
        assert importable.exit_status == 0, (
            "the offline-installed dependency must actually import; a pip exit 0 "
            f"alone does not prove a usable install:\n{importable.output}"
        )

        check = evidence.record(
            _run_in_sandbox("check-no-network", CHECK_COMMAND, workspace=workspace, network="none")
        )
        # Any non-zero exit would satisfy "it failed" -- including an import
        # error from a broken offline install, or an emptied suite. Pin the
        # named seeded failure and the full test count so only the real check
        # run, observing the real defect, can satisfy this leg.
        _assert_unittest_failed_on_the_seeded_defect(check)
        assert _ran_count(check) == _fixture_test_count(), (
            "the offline check must run the WHOLE committed fixture suite; a "
            "smaller run means tests were lost to the offline path rather than "
            f"executed:\n{check.output}"
        )

    evidence.write(_evidence_dir(tmp_path) / "repo-toolchain-vendored-evidence.json")


# --- 6b. Profile B: the live-registry path is genuine -----------------------


def _live_registry_is_reachable(network: str | None) -> bool:
    """Positively establish the skip precondition: is PyPI reachable from the
    sandbox network, right now, before the real install is even attempted.

    Catches the broad-skip regression: pattern-matching pip's failure text
    (``connection``, ``retries exceeded``) also matches TLS, proxy, and
    bad-index-URL failures, so a genuinely broken Profile B command could
    skip instead of fail. This probe answers the question directly and
    cheaply instead of inferring it from a failed install's prose.
    """

    argv = ["docker", "run", "--rm", "--entrypoint", "python"]
    if network is not None:
        argv += ["--network", network]
    argv += [
        RUNNER_IMAGE,
        "-c",
        "import urllib.request; urllib.request.urlopen('https://pypi.org/simple/', timeout=5)",
    ]
    probe = subprocess.run(argv, capture_output=True, text=True, timeout=20, check=False)
    return probe.returncode == 0


def test_live_registry_profile_installs_a_third_party_pin(tmp_path: Path) -> None:
    """Catches Profile B being documented but never actually exercised.

    Profile A proves "only the egress needed" is achievable; this proves the
    live-registry fallback the guide documents really works against real PyPI
    with a genuine third-party pin. It is the one leg coupled to network
    availability, so a confirmed-unreachable registry SKIPS here (the
    deliberate unreachable-registry assertion lives in its own
    negative-control test) -- but the skip precondition is a positive bounded
    connectivity probe, not a pattern match on pip's failure text, so once the
    registry is confirmed reachable, any install failure is a real test
    failure, not a quiet skip. This leg also joins ``CURIE_DOCKER_NETWORK``
    when it is set, so it exercises the product sandbox's actual egress path
    instead of Docker's default bridge; unset, it falls back to the previous
    default-bridge behaviour.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()
    _require_fixture()

    network = os.environ.get("CURIE_DOCKER_NETWORK", "").strip() or None
    if not _live_registry_is_reachable(network):
        pytest.skip("PyPI is unreachable from the sandbox network: connectivity probe failed")

    evidence = Evidence()
    with _owned_tempdir(prefix="curie-2571-live-") as root:
        workspace = _materialize_fixture_clone(Path(root))
        install = evidence.record(
            _run_in_sandbox(
                "install-live-registry",
                INSTALL_LIVE,
                workspace=workspace,
                network=network,
            )
        )
        assert install.exit_status == 0, (
            f"the documented live-registry install must succeed:\n{install.output}"
        )

        importable = evidence.record(
            _run_in_sandbox(
                "import-live-dependency",
                f"{VENV}/bin/python -c 'import packaging; print(packaging.__version__)'",
                workspace=workspace,
                network=network,
            )
        )
        assert importable.exit_status == 0, (
            f"the live-installed third-party pin must import:\n{importable.output}"
        )
        assert "25.0" in importable.stdout, "the installed version must be the pinned one"

    evidence.write(_evidence_dir(tmp_path) / "repo-toolchain-live-registry-evidence.json")


# --- 7. the guide cannot drift away from the harness ------------------------


# The applicability matrix's remaining honesty rows: each is a surface this
# evidence does NOT prove. ``subject`` matches the row's first cell; ``marker`` is the
# not-proved admission the row must still carry. Matched on concept plus marker
# rather than verbatim prose, so rewording is fine and deletion or an upgrade to
# "proved" is not.
_HONESTY_ROWS: tuple[tuple[str, str, str], ...] = (
    (
        "live provider",
        r"live\s+provider",
        r"not\s+covered|not\s+proved|open\b",
    ),
    (
        "GitHub publication approval",
        r"github",
        r"not\s+covered|not\s+proved|open\b|asserted\s+statically|statically",
    ),
    (
        "a persistent workspace volume",
        r"persistent\s+workspace\s+volume",
        r"not\s+the\s+answer|not\s+supported|not\s+proved|unproved|open\b",
    ),
)


_DATA_LOSS_WARNING_TERMS: tuple[tuple[str, str], ...] = (
    ("the pod-replacement case must be named", r"pod\s+replacement|pod-replacing"),
    (
        "uncommitted work must be asserted LOST, not merely mentioned",
        r"uncommitted[^.|\n]{0,80}\b(lost|gone|discard\w*|erase\w*|wipe\w*)\b"
        r"|\b(lost|gone|discard\w*|erase\w*|wipe\w*)\b[^.|\n]{0,80}uncommitted",
    ),
    (
        "an unpublished commit must be asserted LOST too, not just uncommitted edits",
        r"unpublished\s+commit[^.|\n]{0,80}\b(dies|lost|gone|discard\w*)\b",
    ),
    (
        "publication must be named as the durable answer",
        r"publish_changes",
    ),
)

# A warning that says the opposite of what #2615 measured must never pass. These
# are the shapes a softened rewrite actually takes -- "it survives", "it is
# preserved", "it is kept" said of a pod replacement -- and each is a lie about
# the recorded run.
_DATA_LOSS_CONTRADICTIONS: tuple[tuple[str, str], ...] = (
    (
        "uncommitted work claimed to survive a replacement",
        r"pod\s+replacement[^.|\n]{0,120}\b(preserv\w*|surviv\w*|keep\w*|kept|retain\w*)\b"
        r"[^.|\n]{0,60}uncommitted"
        r"|uncommitted[^.|\n]{0,80}\b(preserv\w*|surviv\w*|kept|retain\w*)\b"
        r"[^.|\n]{0,60}pod\s+replacement",
    ),
    (
        "publication declared unnecessary",
        r"no\s+need\s+for\s+publish_changes|publish_changes\s+is\s+(not\s+)?"
        r"(necessary|needed|required)",
    ),
)


def _assert_guide_warns_about_pod_replacement_data_loss(text: str) -> None:
    """Pin the warning that stands between an operator and losing a day's work.

    Issue #2615 proved that a pod-replacing handoff resets ``/workspace`` to the
    published head and discards every in-sandbox commit, uncommitted edit and
    virtualenv. That finding is only worth anything if the guide keeps saying so
    *before* the recipe an operator is about to follow. A drift test that merely
    pinned the applicability row would stay green while the body warning was
    deleted -- the row is read after the fact, the warning is read in time.

    Pinning the *subject words* alone is not enough either, and that is the
    sharper trap: a rewrite saying a pod replacement **preserves** uncommitted
    work names every required term while asserting the opposite of the measured
    result. So each term is pinned to its loss relationship, and the inverted
    claims are rejected outright as negative controls.
    """

    body = text.split("## 3. The recipe")[0]
    assert len(body) < len(text), "the warning must precede the recipe, not follow it"
    lowered = body.casefold()
    for label, pattern in _DATA_LOSS_WARNING_TERMS:
        assert re.search(pattern, lowered), (
            f"{label}: the pod-replacement data-loss warning must appear before "
            "the recipe. Deleting or softening it lets an operator follow the "
            "recipe without being told the workspace is not storage."
        )
    for label, pattern in _DATA_LOSS_CONTRADICTIONS:
        assert not re.search(pattern, lowered), (
            f"{label}: the guide now claims the opposite of what the #2615 "
            "cluster run measured. A pod replacement re-fetches the signed "
            "archive and discards every in-sandbox change; saying otherwise "
            "invites exactly the data loss this warning exists to prevent."
        )
    assert re.search(r"in-place", lowered), (
        "the warning must distinguish an in-place container restart (which "
        "preserves everything) from a pod replacement (which preserves none of "
        "it); without the contrast the reader cannot tell which one they had"
    )


def _assert_guide_discloses_what_is_not_proved(text: str) -> None:
    """Pin the remaining applicability rows that admit unproved surfaces.

    Catches the unpinned-rule class: a drift test that only greps for `local`,
    `cluster` and `slack` stays green after the honesty rows are deleted, which
    is exactly how a guide silently starts overclaiming.
    """

    rows = [line for line in text.splitlines() if line.strip().startswith("|")]
    assert rows, "the guide must carry the applicability matrix as a table"

    for label, subject, marker in _HONESTY_ROWS:
        matched = [row for row in rows if re.search(subject, row, flags=re.IGNORECASE | re.DOTALL)]
        assert matched, (
            f"the applicability matrix must still carry the {label!r} row; "
            "deleting it makes the guide claim more coverage than the evidence has"
        )
        assert any(re.search(marker, row, flags=re.IGNORECASE) for row in matched), (
            f"the {label!r} row must still be marked as open / not proved / not "
            f"covered; upgrading it to a proved claim overclaims. Rows found: {matched}"
        )


def _assert_live_registry_egress_contract(text: str) -> None:
    """Pin the reproducible live-registry check and its CIDR limits."""

    rows = [
        row
        for row in text.splitlines()
        if row.strip().startswith("|")
        and re.search(
            r"live\s+registry\s+dependencies.*(networkpolicy|network\s*policy|egress)",
            row,
            flags=re.IGNORECASE,
        )
    ]
    assert len(rows) == 1, (
        "the applicability matrix must carry exactly one live-registry enforcing-"
        f"NetworkPolicy row; found {rows}"
    )
    row = rows[0].casefold()
    for marker in (
        "admission and refusal",
        "cidr snapshot",
        "packaging==25.0",
        "scripts/check-registry-egress.py",
        "not a domain boundary",
        "not a trusted-registries preset",
    ):
        assert marker in row, (
            f"the live-registry row must retain its contract and limit marker {marker!r}: "
            f"{rows[0]}"
        )

    assert REGISTRY_EGRESS_CHECK.is_file(), (
        f"the documented live-registry check must exist at {REGISTRY_EGRESS_CHECK}"
    )
    assert re.search(
        r"python\s+scripts/check-registry-egress\.py\s+\\?\s*--output-dir\b",
        text,
    ), "the guide must carry the runnable check command with its output directory"


def test_guide_documents_the_recipe_and_the_boundary() -> None:
    """Catches the guide drifting from what the harness actually proves, and the
    honesty rows being quietly deleted so the guide reads as proving more.

    The guide is the deliverable an operator follows; if it stops naming the
    venv location, both dependency strategies, the publication boundary, or the two
    bounded-failure modes, the proof harness is proving something nobody is
    being told to do. And if the applicability matrix loses the rows that admit
    what is *not* proved -- the live provider, GitHub publication approval,
    and the persistent workspace volume -- the guide overclaims. Live registry
    egress has a separate reproducible contract with explicit CIDR limits.

    The restart/handoff row is no longer an honesty row: issue #2615 proved the
    pod-replacing path on a cluster. What replaces that pin is stricter, because
    what that proof found was a way to lose work -- the guide must keep warning,
    before the recipe, that a pod replacement discards everything the sandbox
    made.
    """

    assert GUIDE.is_file(), f"the operator guide must exist at {GUIDE}"
    text = GUIDE.read_text()
    lowered = text.casefold()

    assert VENV in text, f"the guide must name the venv location {VENV}"
    assert "read-only" in lowered and "/tmp" in text and "/home/runner" in text, (
        "the guide must state the sandbox posture the recipe works within"
    )
    assert "--no-index" in text and "--find-links" in text, (
        "the bundled-dependency install command must be copy-pasteable from the guide"
    )
    assert "--allow-web-egress" in text, "the live-registry operator lever must be named"
    assert "0075" in text, "the guide must cite ADR-0075 for the egress imprecision"
    assert "no-new-privileges" in lowered or "cap-drop" in lowered or "cap_drop" in lowered

    assert "publish_changes" in text, "the publication boundary must name the tool"
    assert "do not push with git" in lowered, (
        "the guide must quote the contract's no-git-push instruction"
    )
    assert "github" in lowered and "outside" in lowered, (
        "the guide must state that GitHub write credentials stay outside the sandbox"
    )

    assert "no matching distribution" in lowered or "could not find a version" in lowered, (
        "bounded failure mode 1 (unreachable registry) must be shown"
    )
    assert "command not found" in lowered or "not found" in lowered, (
        "bounded failure mode 2 (missing toolchain) must be shown"
    )
    assert "368" in text, (
        "the guide must record the measured fail-closed live-registry latency, "
        "not describe it only as 'well inside its timeout'"
    )
    assert "/etc/pip.conf" in text and "retries = 0" in text, (
        "the guide must name the image pip.conf that bounds the refusal"
    )
    assert "network is unreachable" in lowered, (
        "the fail-closed default-index error must be shown, not only the "
        "wrong-index 'no matching distribution' text"
    )

    for tier in ("local", "cluster", "slack"):
        assert tier in lowered, f"the applicability matrix must cover the {tier} tier"

    _assert_guide_discloses_what_is_not_proved(text)
    _assert_guide_warns_about_pod_replacement_data_loss(text)
    _assert_live_registry_egress_contract(text)

    # The docs gate bans raw line-coordinate citations anywhere in the file.
    assert not re.search(r"\.(?:py|rs|toml|yaml|md)(?::\d+|#L\d+)", text), (
        "the docs citation gate bans file:line citations; cite by path only"
    )


def test_runner_image_ships_fail_fast_pip_retries() -> None:
    """Catches the image retry bound drifting back to pip's default of 5.

    The 368s cluster refusal was pip's default retry budget. If /etc/pip.conf
    loses ``retries = 0``, or the Dockerfile stops copying it, the naive
    Profile B command hangs for minutes again and the guide's fail-fast claim
    is false.
    """

    assert PIP_CONF.is_file(), f"the runner image pip.conf must exist at {PIP_CONF}"
    conf = PIP_CONF.read_text()
    assert re.search(r"^retries\s*=\s*0\s*$", conf, flags=re.MULTILINE), (
        "runner/pip.conf must set retries = 0; a higher value reintroduces the "
        f"multi-minute fail-closed install:\n{conf}"
    )
    assert re.search(r"^timeout\s*=\s*15\s*$", conf, flags=re.MULTILINE), (
        f"runner/pip.conf must keep pip's 15s socket timeout explicit:\n{conf}"
    )
    dockerfile = DOCKERFILE.read_text()
    assert "COPY runner/pip.conf /etc/pip.conf" in dockerfile, (
        "the Dockerfile must install the pip.conf at the site-wide path "
        "workspace venvs actually read"
    )


def test_live_registry_egress_contract_rejects_material_mutations() -> None:
    """Prove deleting the row or overstating a CIDR snapshot makes the pin red."""

    text = GUIDE.read_text()

    without_row = "\n".join(
        row
        for row in text.splitlines()
        if "Live registry dependencies against an enforcing" not in row
    )
    with pytest.raises(AssertionError):
        _assert_live_registry_egress_contract(without_row)

    overclaimed = text.replace(
        "not a domain boundary and not a trusted-registries preset",
        "a durable domain boundary and a trusted-registries preset",
    )
    assert overclaimed != text, "the mutation must alter the pinned live-registry row"
    with pytest.raises(AssertionError):
        _assert_live_registry_egress_contract(overclaimed)


# --- 8. the deliberately-red fixture must never enter our own suite ---------


def test_fixture_tree_is_not_collected() -> None:
    """Catches the fixture's seeded-red test turning this repository's suite red.

    The fixture ships a test that MUST fail. Two guards keep it out: the
    ``collect_ignore_glob`` in ``runner/tests/conftest.py`` and this assertion.
    """

    _require_fixture()
    fixture_tests = sorted(FIXTURE_REPO.rglob("test_*.py"))
    assert fixture_tests, (
        "the fixture must ship a real, deliberately failing test; without one the "
        "red -> green -> red cycle has nothing to observe"
    )

    conftest = Path(__file__).resolve().parent / "conftest.py"
    assert conftest.is_file(), f"{conftest} must exist to keep the fixture tree out of collection"
    assert 'collect_ignore_glob = ["fixtures/**"]' in conftest.read_text(), (
        "the collection guard must ignore the whole fixtures tree"
    )

    collected = {Path(getattr(mod, "__file__", "") or "") for mod in list(sys.modules.values())}
    for fixture_test in fixture_tests:
        assert fixture_test not in collected, (
            f"{fixture_test} was imported by this pytest run; the collection guard failed"
        )


# --- 9. owned-resource cleanup ----------------------------------------------


def test_no_container_or_tmpdir_survives_the_harness(tmp_path: Path) -> None:
    """Catches the harness leaking the resources it owns.

    Every container is ``--rm`` and every workspace lives in an
    ``_owned_tempdir``. This asserts both properties observably: after a
    round trip, no container this PROCESS actually launched (across the whole
    module, not just this test's synthetic ``true`` leg) is listed (running or
    exited), and the temporary workspace path is gone.

    Regressions caught: (1) a global ``name=curie-2571-proof-`` filter matched
    containers from a concurrent run of this module in a sibling worktree,
    flaking one run on another's containers; ``CONTAINER_PREFIX`` is now
    unique per process. (2) the assertion previously proved cleanliness only
    for one synthetic ``true`` container, so a leak from an earlier real leg
    (install, check, negative-control) would not have been caught; this now
    checks every name recorded in ``_LAUNCHED_CONTAINER_NAMES``.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()

    with _owned_tempdir(prefix="curie-2571-cleanup-") as root:
        workspace = Path(root) / "ws"
        workspace.mkdir()
        os.chmod(workspace, 0o777)
        recorded = workspace
        step = _run_in_sandbox("cleanup-roundtrip", "true", workspace=workspace)
        assert step.exit_status == 0
        assert "--rm" in step.argv, "every container must be self-removing"

    assert not recorded.exists(), "the temporary workspace must be removed"

    listed = subprocess.run(
        ["docker", "ps", "-a", "--filter", f"name={CONTAINER_PREFIX}", "--format", "{{.Names}}"],
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert listed.returncode == 0
    survivors = [line for line in listed.stdout.splitlines() if line.strip()]
    assert not survivors, (
        f"harness containers survived: {survivors}; this process launched "
        f"{len(_LAUNCHED_CONTAINER_NAMES)} containers across the module"
    )
    leaked = set(survivors) & set(_LAUNCHED_CONTAINER_NAMES)
    assert not leaked, f"containers this process actually launched survived cleanup: {leaked}"


def test_host_can_delete_a_workspace_the_container_wrote(tmp_path: Path) -> None:
    """Catches GHA uid 1001 failing to rmtree a uid-1000 ``.venv``.

    The managed runner is uid 1000. A GitHub-hosted runner is uid 1001. Files
    the container writes into the bind-mounted workspace are then not chmod-able
    by the host, and tempfile cleanup raises ``PermissionError`` -- which is
    exactly how the first required-mode CI run went red after every proof
    assertion had already passed.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()

    workspace = tmp_path / "ws"
    workspace.mkdir()
    os.chmod(workspace, 0o777)
    step = _run_in_sandbox(
        "write-venv",
        f"python -m venv {WORKSPACE_MOUNT_PATH}/.venv",
        workspace=workspace,
    )
    assert step.exit_status == 0, f"venv creation must succeed:\n{step.output}"
    venv = workspace / ".venv"
    assert venv.is_dir(), "the container must have written a venv the host can see"
    _force_rmtree(venv)
    assert not venv.exists(), "the host must be able to delete the container-owned venv"


# --- 10. the stdlib wheel builder, checked structurally on the host ----------


def test_stdlib_wheel_builder_produces_a_structurally_valid_wheel(tmp_path: Path) -> None:
    """Catches a broken wheel builder merging because every consumer of it skips.

    Profile A's zero-egress claim rests entirely on this builder: if it emits a
    wheel with a wrong RECORD hash, a missing ``WHEEL``, a non-purelib layout, or
    the package at the wrong archive path, the offline ``pip install --no-index``
    breaks. Its only other consumer is inside the container legs, which skip
    without a runner image -- so this runs the builder as a plain host
    subprocess, with no Docker at all, and therefore always executes on the merge
    gate.
    """

    _require_fixture()
    builder = FIXTURE_REPO / "tools" / "build_wheel.py"
    assert builder.is_file(), f"Profile A's stdlib-only wheel builder must exist at {builder}"

    out_dir = tmp_path / "wheelhouse"
    completed = subprocess.run(
        [sys.executable, str(builder), str(FIXTURE_REPO / "src"), str(out_dir)],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )
    assert completed.returncode == 0, (
        f"the stdlib wheel builder must succeed on the committed source tree:\n"
        f"{completed.stdout}\n{completed.stderr}"
    )

    wheels = sorted(out_dir.glob("*.whl"))
    assert len(wheels) == 1, f"exactly one wheel must be produced; got {wheels}"
    wheel = wheels[0]
    assert wheel.name == f"{FIXTURE_DEP}-1.2.0-py3-none-any.whl", (
        "the wheel filename must carry the pinned name, version and the "
        f"pure-Python tag that {FIXTURE_DEP_PIN} resolves against; got {wheel.name}"
    )

    assert zipfile.is_zipfile(wheel), "the wheel must be a readable zip archive"
    with zipfile.ZipFile(wheel) as archive:
        assert archive.testzip() is None, "no archive member may be corrupt"
        names = set(archive.namelist())

        assert f"{FIXTURE_DEP}/__init__.py" in names, (
            "the importable package must be stored at its import path relative to "
            f"the purelib root; archive holds {sorted(names)}"
        )

        dist_info = f"{FIXTURE_DEP}-1.2.0.dist-info"
        for member in ("METADATA", "WHEEL", "RECORD"):
            assert f"{dist_info}/{member}" in names, (
                f"a PEP 427 wheel must carry {dist_info}/{member}; archive holds {sorted(names)}"
            )

        wheel_metadata = archive.read(f"{dist_info}/WHEEL").decode()
        assert "Root-Is-Purelib: true" in wheel_metadata, (
            "the wheel must declare a purelib root, or pip installs the package "
            f"into platlib and the documented import path breaks:\n{wheel_metadata}"
        )
        assert "Tag: py3-none-any" in wheel_metadata, (
            f"the wheel must declare the pure-Python tag it is named for:\n{wheel_metadata}"
        )

        record = archive.read(f"{dist_info}/RECORD").decode()
        record_lines = [line for line in record.splitlines() if line.strip()]
        assert record_lines, "RECORD must not be empty"
        recorded_paths = set()
        for line in record_lines:
            path, digest, size = line.split(",")
            recorded_paths.add(path)
            if path == f"{dist_info}/RECORD":
                # RECORD cannot hash itself; PEP 427 leaves both fields empty.
                assert digest == "" and size == "", (
                    f"RECORD's own entry must carry an empty hash and size; got {line!r}"
                )
                continue
            assert path in names, f"RECORD names {path!r}, which is not in the archive"
            data = archive.read(path)
            expected = base64.urlsafe_b64encode(hashlib.sha256(data).digest()).rstrip(b"=").decode()
            assert digest == f"sha256={expected}", (
                f"RECORD's hash for {path!r} does not match the archive member; a "
                "wheel whose RECORD lies is not a verifiable install"
            )
            assert size == str(len(data)), (
                f"RECORD's size for {path!r} is {size}, but the member is {len(data)} bytes"
            )

        assert names <= recorded_paths, (
            "every archive member must appear in RECORD; unrecorded members are "
            f"invisible to pip's uninstall: {sorted(names - recorded_paths)}"
        )
