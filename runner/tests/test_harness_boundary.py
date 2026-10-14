"""Enforce the executable boundary around the Claude harness and approval core."""

from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import tomllib
from collections.abc import Iterator
from pathlib import Path

import grimp
import pytest
from curie_runner import __main__ as boot
from curie_runner.harness import registry
from curie_runner.harness.claude import CLAUDE_CONTRIBUTION
from curie_runner.harness.contribution import BundleCompileResult, HarnessContribution

_ROOT = Path(__file__).resolve().parents[2]
_PROBE_MODULE = "curie_runner._boundary_probe"
_SDK_MODULE = "claude_agent_sdk"


@pytest.fixture
def boundary_probe() -> Iterator[Path]:
    """@spec HARNESS-BOUNDARY-ISOLATION-1."""
    with tempfile.TemporaryDirectory(prefix="curie-boundary-") as temporary:
        root = Path(temporary).resolve()
        if root.is_relative_to(_ROOT):
            pytest.fail("boundary controls need a temporary directory outside the checkout")
        shutil.copy2(_ROOT / "pyproject.toml", root / "pyproject.toml")
        for source in _workspace_sources(_ROOT):
            shutil.copytree(
                source,
                root / source.relative_to(_ROOT),
                ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
            )
        yield root / "runner/src/curie_runner/_boundary_probe.py"


@pytest.fixture
def lint_imports_command(boundary_probe: Path) -> str:
    """@spec HARNESS-BOUNDARY-ISOLATION-1."""
    command = shutil.which("lint-imports")
    if command is None:
        pytest.fail("lint-imports must be installed to verify the harness boundary")
    baseline = _lint_imports(command, boundary_probe.parents[3])
    if baseline.returncode != 0:
        pytest.fail("the linter baseline is unavailable:\n" + baseline.stdout + baseline.stderr)
    return command


def _lint_imports(command: str, root: Path) -> subprocess.CompletedProcess[str]:
    """@spec HARNESS-BOUNDARY-ISOLATION-1."""
    return subprocess.run(
        [command, "--no-cache", "--no-logo"],
        cwd=root,
        env=os.environ | {
            "PYTHONPATH": os.pathsep.join(str(path) for path in _workspace_sources(root))
        },
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
    )


def test_actual_linter_refuses_a_new_sdk_importer_and_accepts_its_control(
    boundary_probe: Path, lint_imports_command: str
) -> None:
    """@spec HARNESS-BOUNDARY-ISOLATION-1."""
    boundary_probe.write_text("BOUNDARY_CONTROL = True\n", encoding="utf-8")
    control = _lint_imports(lint_imports_command, boundary_probe.parents[3])
    assert control.returncode == 0, control.stdout + control.stderr

    boundary_probe.write_text("import claude_agent_sdk\n", encoding="utf-8")
    rejected = _lint_imports(lint_imports_command, boundary_probe.parents[3])
    output = rejected.stdout + rejected.stderr
    assert rejected.returncode != 0, output
    assert _PROBE_MODULE in output, output
    assert _SDK_MODULE in output, output


def _workspace_sources(root: Path) -> list[Path]:
    """@spec HARNESS-BOUNDARY-ISOLATION-1."""
    config = tomllib.loads((root / "pyproject.toml").read_text(encoding="utf-8"))
    return [root / member / "src" for member in config["tool"]["uv"]["workspace"]["members"]]


def _source_snapshot() -> dict[Path, bytes]:
    """@spec HARNESS-BOUNDARY-ISOLATION-1."""
    paths = [path for source in _workspace_sources(_ROOT) for path in source.rglob("*.py")]
    paths.append(_ROOT / "pyproject.toml")
    return {path.relative_to(_ROOT): path.read_bytes() for path in paths}


@pytest.mark.parametrize("forbidden", [False, True], ids=["positive", "negative"])
def test_boundary_controls_leave_authoritative_sources_unchanged_while_probe_exists(
    forbidden: bool, boundary_probe: Path, lint_imports_command: str
) -> None:
    """@spec HARNESS-BOUNDARY-ISOLATION-1."""
    before = _source_snapshot()
    boundary_probe.write_text(
        "import claude_agent_sdk\n" if forbidden else "BOUNDARY_CONTROL = True\n",
        encoding="utf-8",
    )
    probe_root = boundary_probe.parents[3]
    environment = os.environ | {
        "PYTHONPATH": os.pathsep.join(str(path) for path in _workspace_sources(probe_root))
    }
    with subprocess.Popen(
        [lint_imports_command, "--no-cache", "--no-logo"],
        cwd=probe_root,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    ) as process:
        during = _source_snapshot()
        try:
            stdout, stderr = process.communicate(timeout=120)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise
    output = stdout + stderr
    assert (process.returncode != 0) is forbidden, output
    if forbidden:
        assert _PROBE_MODULE in output, output
        assert _SDK_MODULE in output, output
    assert boundary_probe.exists()
    assert during == before, "the running linter's boundary probe changed authoritative sources"
    assert _source_snapshot() == before, "the active boundary probe changed authoritative sources"


def test_approval_core_has_no_direct_or_transitive_sdk_dependency() -> None:
    graph = grimp.build_graph("curie_runner", include_external_packages=True, cache_dir=None)
    assert "curie_runner.approval" in graph.modules
    assert _SDK_MODULE in graph.modules
    sdk_path = graph.find_shortest_chain("curie_runner.approval", _SDK_MODULE)
    assert sdk_path is None, sdk_path


def test_registered_alternate_is_refused_before_registry_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    selected = "rival"
    alternate = HarnessContribution(
        name=selected,
        readonly_tools=frozenset(),
        build_spawn_env=lambda env: None,
        compile_bundle=lambda plugin_dir: BundleCompileResult(plugins=[], system_prompt=None),
    )
    discovery_calls: list[None] = []

    def discover() -> dict[str, HarnessContribution]:
        discovery_calls.append(None)
        return {selected: alternate}

    monkeypatch.setattr(registry, "discover_contributions", discover)
    with pytest.raises(RuntimeError, match=selected) as excinfo:
        boot._resolve_harness(selected)
    assert excinfo.type.__name__ == "UnsupportedHarnessError"
    assert discovery_calls == []


@pytest.mark.parametrize("selected", ["claude", "claude-sdk", "claude-code"])
def test_claude_and_aliases_resolve_without_registry_discovery(
    selected: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unavailable_registry() -> dict[str, HarnessContribution]:
        pytest.fail("builtin Claude boot must not discover alternate registrations")

    monkeypatch.setattr(registry, "discover_contributions", unavailable_registry)
    assert boot._resolve_harness(selected) is CLAUDE_CONTRIBUTION


def test_process_boot_refuses_non_claude_before_opening_the_listener(tmp_path: Path) -> None:
    plugin = tmp_path / ".claude-plugin"
    plugin.mkdir()
    (plugin / "plugin.json").write_text(json.dumps({"name": "boundary"}), encoding="utf-8")
    environment = {
        key: value
        for key, value in os.environ.items()
        if not key.startswith(("CURIE_", "OTEL_", "LANGFUSE_"))
    }
    # Holding the configured port makes an attempted listener a distinct failure.
    with socket.socket() as occupied_port:
        occupied_port.bind(("127.0.0.1", 0))
        occupied_port.listen()
        environment.update(
            {
                "CURIE_PLUGIN_DIR": str(tmp_path),
                "CURIE_SESSION_ID": "s-boundary",
                "CURIE_SANDBOX_ID": "b-boundary",
                "CURIE_BUDGET": '{"max_output_tokens_per_run":10000,"max_usd_per_day":1.0}',
                "CURIE_FAKE_MODEL": "1",
                "CURIE_HARNESS": "rival",
                "CURIE_RUNNER_TOKEN": "boundary-control-placeholder-token",
                "CURIE_RUNNER_PORT": str(occupied_port.getsockname()[1]),
                "OTEL_SDK_DISABLED": "true",
            }
        )
        result = subprocess.run(
            [sys.executable, "-m", "curie_runner"],
            cwd=_ROOT,
            env=environment,
            capture_output=True,
            text=True,
            timeout=30,
            check=False,
        )

    output = result.stdout + result.stderr
    assert result.returncode != 0, output
    assert "UnsupportedHarnessError" in output, output
    assert "rival" in output, output
    assert "address already in use" not in output.lower(), output
    assert "session started" not in output, output
    assert "Running on" not in output, output
