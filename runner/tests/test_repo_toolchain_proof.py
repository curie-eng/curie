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

- no ``docker`` binary, or a binary with an unreachable daemon -> **skip**;
- the resolved runner image absent locally -> **skip** with a message naming
  ``curie build``;
- hardening disabled (``run_args()`` empty) -> **fail**, because a vacuous proof
  is worse than no proof.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURE_REPO = Path(__file__).resolve().parent / "fixtures" / "repo_toolchain"
GUIDE = REPO_ROOT / "docs" / "guides" / "repository-toolchain-in-the-managed-sandbox.md"

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

CONTAINER_PREFIX = "curie-2571-proof-"
DEFAULT_TIMEOUT = 300


# --- gates -------------------------------------------------------------------


def _require_docker() -> None:
    if shutil.which("docker") is None:
        pytest.skip("Docker is unavailable: docker CLI is not installed")
    probe = subprocess.run(
        ["docker", "info", "--format", "{{.ServerVersion}}"],
        capture_output=True,
        text=True,
        timeout=30,
        check=False,
    )
    if probe.returncode != 0:
        reason = probe.stderr.strip() or probe.stdout.strip()
        pytest.skip(f"Docker is unavailable: {reason}")


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
        pytest.skip(
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
) -> Step:
    """Run one shell script inside the runner image under the product posture.

    ``--rm`` is unconditional: owned-resource cleanup is itself an AC.
    """

    name = f"{CONTAINER_PREFIX}{uuid.uuid4().hex[:12]}"
    argv = ["docker", "run", "--rm", "--name", name, *HARDENING_ARGS]
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


# --- 1. the red -> green -> red cycle ---------------------------------------


def test_seeded_defect_fails_then_fix_passes_then_revert_fails_again(tmp_path: Path) -> None:
    """Catches a "check command" that is not actually checking anything.

    A single red or a single green cannot distinguish a real test command from
    one that always fails (or always passes). Running the *same* command string
    across defect -> fix -> revert, and requiring the exit statuses to move
    non-zero -> zero -> non-zero, is what makes the claim falsifiable. The
    per-leg commit SHA is recorded so the PR evidence names the exact tree each
    status belongs to.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()
    _require_fixture()

    evidence = Evidence()
    with tempfile.TemporaryDirectory(prefix="curie-2571-cycle-") as root:
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
        assert red.exit_status != 0, (
            f"the repository's documented check must FAIL on the seeded defect:\n{red.output}"
        )

        patch = workspace / "FIX.patch"
        assert patch.is_file(), f"the fixture must ship the one-line fix at {patch}"
        _host_git(workspace, "apply", "FIX.patch")
        _host_git(workspace, "commit", "-q", "-a", "-m", "Apply the one-line fix")
        fix_sha = _host_git(workspace, "rev-parse", "HEAD")

        green = evidence.record(_run_in_sandbox("check-on-fix", CHECK_COMMAND, workspace=workspace))
        green.commit_sha = fix_sha
        assert green.exit_status == 0, (
            f"the same documented check must PASS once the defect is fixed:\n{green.output}"
        )

        _host_git(workspace, "revert", "--no-edit", "-n", fix_sha)
        _host_git(workspace, "commit", "-q", "-a", "-m", "Revert the one-line fix")
        revert_sha = _host_git(workspace, "rev-parse", "HEAD")

        red_again = evidence.record(
            _run_in_sandbox("check-on-revert", CHECK_COMMAND, workspace=workspace)
        )
        red_again.commit_sha = revert_sha
        assert red_again.exit_status != 0, (
            f"reverting the fix must restore the failure; a check that stays green "
            f"here is not observing the code:\n{red_again.output}"
        )

        assert defect_sha != fix_sha != revert_sha, (
            "each leg must have a distinct commit identity so the recorded exit "
            "statuses are attributable to specific trees"
        )

    evidence.write(tmp_path / "repo-toolchain-cycle-evidence.json")


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
    with tempfile.TemporaryDirectory(prefix="curie-2571-install-") as root:
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

    evidence.write(tmp_path / "repo-toolchain-install-evidence.json")


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
    with tempfile.TemporaryDirectory(prefix="curie-2571-noexec-") as root:
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

    evidence.write(tmp_path / "repo-toolchain-noexec-evidence.json")


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
    with tempfile.TemporaryDirectory(prefix="curie-2571-registry-") as root:
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

    evidence.write(tmp_path / "repo-toolchain-unreachable-registry-evidence.json")


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
    with tempfile.TemporaryDirectory(prefix="curie-2571-toolchain-") as root:
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

    evidence.write(tmp_path / "repo-toolchain-missing-toolchain-evidence.json")


# --- 6. Profile A: the "only" in "only the egress needed" --------------------


def test_vendored_profile_needs_no_registry_egress(tmp_path: Path) -> None:
    """Catches Profile A quietly acquiring a dependency on registry egress.

    AC bullet 3 asks for *only* the package-registry egress the repository
    needs. Profile A's answer is "none", and ``--network none`` is what proves
    it by construction: the fixture's dependency is built from committed source
    by a stdlib-only wheel builder and installed with
    ``--no-index --find-links``. If the builder, the source tree, or the offline
    install path breaks, this goes red -- which is the wanted signal, not a skip.
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
    with tempfile.TemporaryDirectory(prefix="curie-2571-vendored-") as root:
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
        assert check.exit_status != 0, (
            "the seeded defect must still be observed offline, proving the check "
            f"really ran rather than being skipped:\n{check.output}"
        )

    evidence.write(tmp_path / "repo-toolchain-vendored-evidence.json")


# --- 6b. Profile B: the live-registry path is genuine -----------------------


def test_live_registry_profile_installs_a_third_party_pin(tmp_path: Path) -> None:
    """Catches Profile B being documented but never actually exercised.

    Profile A proves "only the egress needed" is achievable; this proves the
    live-registry fallback the guide documents really works against real PyPI
    with a genuine third-party pin. It is the one leg coupled to network
    availability, so an unreachable registry SKIPS here (the deliberate
    unreachable-registry assertion lives in its own negative-control test).
    """

    _require_runner_image()
    _require_non_vacuous_hardening()
    _require_fixture()

    evidence = Evidence()
    with tempfile.TemporaryDirectory(prefix="curie-2571-live-") as root:
        workspace = _materialize_fixture_clone(Path(root))
        install = evidence.record(
            _run_in_sandbox(
                "install-live-registry",
                INSTALL_LIVE,
                workspace=workspace,
                network=None,
            )
        )
        lowered = install.output.casefold()
        if install.exit_status != 0 and (
            "temporary failure in name resolution" in lowered
            or "network is unreachable" in lowered
            or "connection" in lowered
            or "retries exceeded" in lowered
        ):
            pytest.skip(f"PyPI is unreachable from this host: {install.output.strip()[-300:]}")
        assert install.exit_status == 0, (
            f"the documented live-registry install must succeed:\n{install.output}"
        )

        importable = evidence.record(
            _run_in_sandbox(
                "import-live-dependency",
                f"{VENV}/bin/python -c 'import packaging; print(packaging.__version__)'",
                workspace=workspace,
                network=None,
            )
        )
        assert importable.exit_status == 0, (
            f"the live-installed third-party pin must import:\n{importable.output}"
        )
        assert "25.0" in importable.stdout, "the installed version must be the pinned one"

    evidence.write(tmp_path / "repo-toolchain-live-registry-evidence.json")


# --- 7. the guide cannot drift away from the harness ------------------------


def test_guide_documents_the_recipe_and_the_boundary() -> None:
    """Catches the guide drifting from what the harness actually proves.

    The guide is the deliverable an operator follows; if it stops naming the
    venv location, both egress profiles, the publication boundary, or the two
    bounded-failure modes, the proof harness is proving something nobody is
    being told to do.
    """

    assert GUIDE.is_file(), f"the operator guide must exist at {GUIDE}"
    text = GUIDE.read_text()
    lowered = text.casefold()

    assert VENV in text, f"the guide must name the venv location {VENV}"
    assert "read-only" in lowered and "/tmp" in text and "/home/runner" in text, (
        "the guide must state the sandbox posture the recipe works within"
    )
    assert "--no-index" in text and "--find-links" in text, (
        "Profile A's vendored install command must be copy-pasteable from the guide"
    )
    assert "--allow-web-egress" in text, "Profile B's operator lever must be named"
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

    for tier in ("local", "cluster", "slack"):
        assert tier in lowered, f"the applicability matrix must cover the {tier} tier"

    # The docs gate bans raw line-coordinate citations anywhere in the file.
    import re as _re

    assert not _re.search(r"\.(?:py|rs|toml|yaml|md)(?::\d+|#L\d+)", text), (
        "the docs citation gate bans file:line citations; cite by path only"
    )


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

    import sys

    collected = {Path(getattr(mod, "__file__", "") or "") for mod in list(sys.modules.values())}
    for fixture_test in fixture_tests:
        assert fixture_test not in collected, (
            f"{fixture_test} was imported by this pytest run; the collection guard failed"
        )


# --- 9. owned-resource cleanup ----------------------------------------------


def test_no_container_or_tmpdir_survives_the_harness(tmp_path: Path) -> None:
    """Catches the harness leaking the resources it owns.

    Every container is ``--rm`` and every workspace lives in a
    ``TemporaryDirectory``. This asserts both properties observably: after a
    round trip, no container carrying the harness prefix is listed (running or
    exited) and the temporary workspace path is gone.
    """

    _require_runner_image()
    _require_non_vacuous_hardening()

    with tempfile.TemporaryDirectory(prefix="curie-2571-cleanup-") as root:
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
    assert not survivors, f"harness containers survived: {survivors}"
