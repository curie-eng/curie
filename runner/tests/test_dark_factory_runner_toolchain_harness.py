"""Service-free tests for the factory runner proof's namespace ownership helpers.

A stub `kubectl` on PATH stands in for the cluster. The stub's error texts follow
kubectl's standard `Error from server (<Reason>): ...` form for API status errors
and its `Unable to connect to the server: ...` form for transport failures.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT / "tools/runner-toolchain-proof"))

import toolchain_proof  # noqa: E402

CONTEXT = "proof-context"
IDENTITY = "unit-1"
NAMESPACE = "toolchain-proof-unit-1"

STUB = """\
#!{python}
import json
import os
import os
import sys

argv = sys.argv[1:]
log = os.environ["STUB_LOG"]
with open(log, "a") as handle:
    handle.write(json.dumps(argv) + "\\n")

# Drop the leading --context <ctx> pair to find the verb.
rest = argv[2:] if argv[:1] == ["--context"] else argv
while rest[:1] == ["-n"]:
    rest = rest[2:]
verb = rest[0] if rest else ""
name = rest[2] if len(rest) > 2 else ""


def fail(text):
    sys.stderr.write(text + "\\n")
    sys.exit(1)


NOT_FOUND = 'Error from server (NotFound): namespaces "%s" not found' % name
TRANSPORT = (
    "Unable to connect to the server: dial tcp 127.0.0.1:1: connect: connection refused"
)
FORBIDDEN = (
    'Error from server (Forbidden): namespaces "%s" is forbidden: User "tester" '
    'cannot get resource "namespaces" in API group "" at the cluster scope' % name
)

if verb == "get" and rest[1:2] == ["namespace"]:
    mode = os.environ.get("STUB_GET", "notfound")
    if mode == "exists":
        print("namespace/" + name)
    elif mode == "notfound":
        fail(NOT_FOUND)
    elif mode == "transport":
        fail(TRANSPORT)
    elif mode == "forbidden":
        fail(FORBIDDEN)
    elif mode.startswith("exists_then_notfound:"):
        limit = int(mode.split(":", 1)[1])
        counter = log + ".count"
        try:
            seen = int(open(counter).read())
        except FileNotFoundError:
            seen = 0
        with open(counter, "w") as handle:
            handle.write(str(seen + 1))
        if seen < limit:
            print("namespace/" + name)
        else:
            fail(NOT_FOUND)
    else:
        fail("unknown STUB_GET " + mode)
elif verb == "create" and rest[1:2] == ["namespace"]:
    if os.environ.get("STUB_CREATE") == "exists":
        fail('Error from server (AlreadyExists): namespaces "%s" already exists' % name)
elif verb == "delete":
    if os.environ.get("STUB_DELETE") == "transport":
        fail(TRANSPORT)
"""


@pytest.fixture
def stub(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Install the stub kubectl first on PATH and return its call log path."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    executable = bin_dir / "kubectl"
    executable.write_text(STUB.format(python=sys.executable), encoding="utf-8")
    executable.chmod(0o755)
    log = tmp_path / "kubectl.log"
    log.touch()
    monkeypatch.setenv("PATH", f"{bin_dir}:{os.environ['PATH']}")
    monkeypatch.setenv("STUB_LOG", str(log))
    for name in ("STUB_GET", "STUB_CREATE", "STUB_DELETE"):
        monkeypatch.delenv(name, raising=False)
    return log


def _calls(log: Path) -> list[list[str]]:
    return [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]


def _verbs(log: Path) -> list[str]:
    verbs = []
    for call in _calls(log):
        rest = call[2:]
        while rest[:1] == ["-n"]:
            rest = rest[2:]
        verbs.append(rest[0])
    return verbs


def _target() -> toolchain_proof.ProofTarget:
    return toolchain_proof.resolve_target(
        {
            toolchain_proof.CONTEXT_ENV: CONTEXT,
            toolchain_proof.IDENTITY_ENV: IDENTITY,
        }
    )


def _owned() -> toolchain_proof.OwnedNamespace:
    return toolchain_proof.OwnedNamespace(context=CONTEXT, name=NAMESPACE)


def _delete(**kwargs: float) -> object:
    """Run cleanup and return its result so liveness tests can assert it is None."""
    return toolchain_proof.delete_owned_namespace(  # type: ignore[func-returns-value]
        _owned(), **{"deadline_seconds": 2, "poll_seconds": 0, **kwargs}
    )


@pytest.mark.parametrize(
    ("env", "needle"),
    [
        ({toolchain_proof.IDENTITY_ENV: IDENTITY}, toolchain_proof.CONTEXT_ENV),
        ({toolchain_proof.CONTEXT_ENV: CONTEXT}, toolchain_proof.IDENTITY_ENV),
        ({}, toolchain_proof.CONTEXT_ENV),
        (
            {toolchain_proof.CONTEXT_ENV: "  ", toolchain_proof.IDENTITY_ENV: IDENTITY},
            toolchain_proof.CONTEXT_ENV,
        ),
        (
            {toolchain_proof.CONTEXT_ENV: CONTEXT, toolchain_proof.IDENTITY_ENV: ""},
            toolchain_proof.IDENTITY_ENV,
        ),
        (
            {toolchain_proof.CONTEXT_ENV: CONTEXT, toolchain_proof.IDENTITY_ENV: "Upper"},
            toolchain_proof.IDENTITY_ENV,
        ),
        (
            {toolchain_proof.CONTEXT_ENV: CONTEXT, toolchain_proof.IDENTITY_ENV: "a" * 60},
            toolchain_proof.IDENTITY_ENV,
        ),
        (
            {toolchain_proof.CONTEXT_ENV: CONTEXT, toolchain_proof.IDENTITY_ENV: "edge-"},
            toolchain_proof.IDENTITY_ENV,
        ),
    ],
)
def test_resolve_target_rejects_missing_or_invalid_input(env: dict[str, str], needle: str) -> None:
    with pytest.raises(toolchain_proof.ProofConfigError, match=needle):
        toolchain_proof.resolve_target(env)


def test_resolve_target_derives_the_namespace_from_the_identity() -> None:
    target = _target()
    assert target.context == CONTEXT
    assert target.identity == IDENTITY
    assert target.namespace == NAMESPACE


SECRET_KEY = "CURIE_PROBE_UNRELATED_CREDENTIAL"
SECRET_VALUE = "sentinel-credential-value-4f2a9c"


def test_missing_context_failure_carries_no_other_environment_key() -> None:
    env = {toolchain_proof.IDENTITY_ENV: IDENTITY, SECRET_KEY: SECRET_VALUE}
    with pytest.raises(toolchain_proof.ProofConfigError) as raised:
        toolchain_proof.resolve_target(env)
    for text in (str(raised.value), repr(raised.value)):
        assert toolchain_proof.CONTEXT_ENV in text
        assert SECRET_KEY not in text
        assert SECRET_VALUE not in text


def test_live_proof_config_failure_traceback_does_not_print_the_process_environment(
    tmp_path: Path,
) -> None:
    """A missing-config failure must not dump the environment, even with locals shown.

    The live test runs in a child pytest with `-l` (show locals) and no context. An
    unrelated credential is exported to that child. Passing the whole environment
    into the resolver would print it in the failing frame's locals.
    """
    # The credential comes first so it sits inside the truncated repr pytest
    # prints for a large mapping.
    env = {
        SECRET_KEY: SECRET_VALUE,
        **os.environ,
        toolchain_proof.PROOF_ENV: toolchain_proof.PROOF_REQUIRED,
        "CURIE_FACTORY_RUNNER_IMAGE": "registry.invalid/runner:0",
        toolchain_proof.IDENTITY_ENV: IDENTITY,
    }
    env.pop(toolchain_proof.CONTEXT_ENV, None)
    live = REPO_ROOT / "runner/tests/test_dark_factory_runner_toolchain.py"
    child = subprocess.run(
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-l",
            "-p",
            "no:cacheprovider",
            f"{live}::test_factory_runner_pod_resolves_repository_lockfiles",
        ],
        cwd=tmp_path,
        env=env,
        capture_output=True,
        text=True,
        check=False,
        timeout=120,
    )
    output = child.stdout + child.stderr
    assert child.returncode != 0, output
    assert toolchain_proof.CONTEXT_ENV in output, output
    assert SECRET_VALUE not in output
    assert SECRET_KEY not in output


def test_create_owned_namespace_returns_ownership_when_absent(
    stub: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_GET", "notfound")
    owned = toolchain_proof.create_owned_namespace(_target())
    assert owned == _owned()
    assert _verbs(stub) == ["get", "create"]


def test_preexisting_namespace_is_never_adopted_or_deleted(
    stub: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_GET", "exists")
    with pytest.raises(toolchain_proof.NamespaceExists):
        toolchain_proof.create_owned_namespace(_target())
    assert _verbs(stub) == ["get"]


def test_create_race_fails_closed_without_deleting(
    stub: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_GET", "notfound")
    monkeypatch.setenv("STUB_CREATE", "exists")
    with pytest.raises(toolchain_proof.NamespaceExists):
        toolchain_proof.create_owned_namespace(_target())
    assert "delete" not in _verbs(stub)


@pytest.mark.parametrize(
    ("mode", "kind"), [("transport", "transport"), ("forbidden", "authorization")]
)
def test_probe_failure_is_not_treated_as_absent(
    stub: Path, monkeypatch: pytest.MonkeyPatch, mode: str, kind: str
) -> None:
    monkeypatch.setenv("STUB_GET", mode)
    with pytest.raises(toolchain_proof.NamespaceProbeError, match=kind):
        toolchain_proof.create_owned_namespace(_target())
    assert _verbs(stub) == ["get"]


@pytest.mark.parametrize(
    ("mode", "kind"), [("transport", "transport"), ("forbidden", "authorization")]
)
def test_cleanup_is_unverified_unless_not_found_is_observed(
    stub: Path, monkeypatch: pytest.MonkeyPatch, mode: str, kind: str
) -> None:
    monkeypatch.setenv("STUB_GET", mode)
    with pytest.raises(toolchain_proof.CleanupUnverified, match=kind) as raised:
        _delete()
    assert NAMESPACE in str(raised.value)
    assert _verbs(stub) == ["delete", "get"]


def test_failed_delete_is_unverified(stub: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("STUB_DELETE", "transport")
    monkeypatch.setenv("STUB_GET", "notfound")
    with pytest.raises(toolchain_proof.CleanupUnverified, match="transport"):
        _delete()


def test_cleanup_succeeds_when_not_found_is_observed(
    stub: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_GET", "notfound")
    assert _delete() is None
    assert _verbs(stub) == ["delete", "get"]


def test_cleanup_polls_until_the_namespace_is_gone(
    stub: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_GET", "exists_then_notfound:2")
    assert _delete() is None
    assert _verbs(stub) == ["delete", "get", "get", "get"]


def test_cleanup_fails_when_the_namespace_remains(
    stub: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("STUB_GET", "exists")
    with pytest.raises(toolchain_proof.CleanupUnverified, match="remained"):
        _delete(deadline_seconds=0.05, poll_seconds=0.01)


def _git_repo(root: Path) -> Path:
    repo = root / "repo"
    repo.mkdir()
    (repo / "file.txt").write_text("content\n", encoding="utf-8")
    identity = ["-c", "user.name=Tester", "-c", "user.email=tester@example.invalid"]
    for args in (["init", "-q"], ["add", "file.txt"], [*identity, "commit", "-q", "-m", "init"]):
        subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True)
    return repo


def test_every_kubectl_call_carries_the_context(
    stub: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("STUB_GET", "notfound")
    owned = toolchain_proof.create_owned_namespace(_target())
    toolchain_proof.copy_checkout(owned, "pod", "staging", _git_repo(tmp_path))
    _delete()
    calls = _calls(stub)
    assert _verbs(stub) == ["get", "create", "exec", "delete", "get"]
    for call in calls:
        assert call[:2] == ["--context", CONTEXT], call


def test_copy_checkout_reports_a_failed_copy(
    stub: Path, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # An empty directory is not a git repository, so the archive step fails.
    empty = tmp_path / "empty"
    empty.mkdir()
    with pytest.raises(RuntimeError, match="archive"):
        toolchain_proof.copy_checkout(_owned(), "pod", "staging", empty)


def _result(stderr: str, returncode: int = 1) -> subprocess.CompletedProcess[str]:
    return subprocess.CompletedProcess(["kubectl"], returncode, stdout="", stderr=stderr)


@pytest.mark.parametrize(
    ("stderr", "kind"),
    [
        ('Error from server (NotFound): namespaces "x" not found', "not_found"),
        ('Error from server (Forbidden): namespaces "x" is forbidden', "authorization"),
        ("error: You must be logged in (Unauthorized)", "authorization"),
        ("Unable to connect to the server: connection refused", "transport"),
        ("", "transport"),
        ("some unrecognised failure", "transport"),
    ],
)
def test_classify_failure(stderr: str, kind: str) -> None:
    assert toolchain_proof.classify_failure(_result(stderr)) == kind
