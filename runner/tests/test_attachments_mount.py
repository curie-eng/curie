"""The agent can actually OPEN an inbound attachment (#2567, S4).

The worker parks the bytes and the chart's ``attachments-init`` materializes
them, and both of those are invisible to the model unless the runner does two
things: put the files somewhere a tool call can reach from the session's working
directory, and TELL the model they are there. A file the agent is never told
about is indistinguishable from a file that never arrived -- the agent answers
"I don't see an attachment" about a message that visibly carries one, which is
the exact user-facing bug this stage closes.

Two decisions are pinned here, both by construction rather than by preference:

* **Attachments land in their OWN mount, a SIBLING of the workspace**, at
  ``ATTACHMENTS_DIR``. Not inside ``/workspace``: ``workspace-init`` deletes
  every child of its root on entry (so a restart cannot overlay a partial
  extraction), which would silently eat the files depending on init order; and
  anything under the checkout shows up in ``git status``, so a person's
  spreadsheet could ride into a publication diff.

* **The model is told by absolute path.** The session's cwd is the workspace
  (``adapter.build_options(cwd=...)``), so a bare filename resolves to
  ``/workspace/<name>`` and fails. Naming the file without a path that resolves
  is the same bug as not naming it at all, one step later.

The absent case is asserted as strictly as the present one: a turn with no
attachments -- the overwhelming majority -- must produce the session options it
produces today, byte for byte.
"""

from __future__ import annotations

import json
import typing
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import yaml
from aiohttp import web
from curie_runner import __main__ as boot
from curie_runner.__main__ import build_runner
from curie_runner.config import RunnerConfig

_BUDGET = '{"max_output_tokens_per_run": 10000, "max_usd_per_day": 1.0}'


def _attachments_dir() -> Path:
    """The compiled-in mount, resolved at call time rather than at import.

    Read through ``getattr`` so a module that does not declare it yet fails
    these two tests with the reason, instead of an ImportError that aborts
    collection for the whole runner suite.
    """

    directory = getattr(boot, "ATTACHMENTS_DIR", None)
    assert isinstance(directory, Path), (
        "curie_runner.__main__ declares no ATTACHMENTS_DIR. Nothing at runtime "
        "tells the runner where the attachment volume was mounted -- the "
        "reference env is scoped away from the runner container -- so the path "
        "has to be compiled in."
    )
    return directory


class _CapturedSession:
    """Stands in for ClaudeAgentSession so the built options can be inspected.

    Same stand-in as ``test_approval_gate_enforcement``: the real class
    constructs an SDK client, and the assertions here are about what
    ``build_runner`` PUT in the options.
    """

    def __init__(self, options: Any) -> None:
        self.options = options

    async def connect(self) -> None:
        return None

    async def query(self, _text: str) -> None:
        return None

    async def interrupt(self) -> None:
        return None

    async def close(self) -> None:
        return None

    async def receive_turn(self) -> typing.AsyncIterator[Any]:
        if False:
            yield None


def _bundle(root: Path) -> str:
    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps({"name": "attachments-demo", "version": "0.1.0"}),
        encoding="utf-8",
    )
    return str(root)


def _config(plugin_dir: str) -> RunnerConfig:
    return RunnerConfig.from_env(
        {
            "CURIE_PLUGIN_DIR": plugin_dir,
            "CURIE_SESSION_ID": "s-2567",
            "CURIE_SANDBOX_ID": "b-2567",
            "CURIE_BUDGET": _BUDGET,
        }
    )


def _workspace(root: Path) -> Path:
    workspace = root / "workspace"
    (workspace / ".git").mkdir(parents=True)
    return workspace


def _attachments(root: Path, files: dict[str, bytes]) -> Path:
    mount = root / "attachments"
    mount.mkdir(parents=True, exist_ok=True)
    for name, payload in files.items():
        (mount / name).write_bytes(payload)
    return mount


def _options(
    monkeypatch: pytest.MonkeyPatch,
    config: RunnerConfig,
    **kwargs: Any,
) -> Any:
    monkeypatch.setattr(boot, "ClaudeAgentSession", _CapturedSession)
    runner = build_runner(config, **kwargs)
    session = runner._factory()  # noqa: SLF001 -- the boot wiring is the subject
    assert isinstance(session, _CapturedSession)
    return session.options


# --- where the files land ---------------------------------------------------


def test_the_attachment_mount_is_a_sibling_of_the_workspace_not_inside_it() -> None:
    # revert: point ATTACHMENTS_DIR at /workspace/<anything> -> this fails, and
    # so does the property it protects: workspace-init wipes every child of its
    # root on entry, and a file under the checkout rides into `git status`.
    mount = _attachments_dir()
    assert mount.is_absolute()
    assert not mount.is_relative_to(Path("/workspace"))
    assert mount != Path("/workspace")


def test_the_runner_default_matches_the_chart_mount_path() -> None:
    """The cross-language seam: the chart mounts the emptyDir, the runner reads it.

    Nothing at runtime tells the runner where the volume was mounted -- the
    reference env is scoped to ``attachments-init`` and never reaches the runner
    container -- so the path is compiled in on one side and templated on the
    other. Moving one alone mounts a volume nothing reads, and the only symptom
    is an agent that cannot see files that are sitting in the pod.

    Resolved from this file's location so it holds from the repo root or from
    ``runner/``.
    """

    mount = _attachments_dir()
    repo_root = Path(__file__).resolve().parents[2]
    values = yaml.safe_load((repo_root / "charts" / "curie" / "values.yaml").read_text())
    attachments = values["agentSandbox"]["runner"].get("attachments")
    assert attachments is not None, (
        "charts/curie/values.yaml declares no agentSandbox.runner.attachments, "
        "so nothing mounts the volume the runner reads"
    )

    assert attachments["mountPath"] == str(mount), (
        f"charts/curie/values.yaml mounts the attachments emptyDir at "
        f"{attachments['mountPath']!r} but curie_runner reads {str(mount)!r}. "
        f"They are one path expressed in two languages and must move together."
    )


# --- the model is told, by a path that resolves -----------------------------


def test_the_agent_is_told_each_attachment_by_a_path_it_can_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # revert: materialize the files and pass no preamble -> the files are on
    # disk, the agent is never told, and it reports it cannot see them.
    plugin_dir = _bundle(tmp_path / "plugin")
    workspace = _workspace(tmp_path)
    mount = _attachments(
        tmp_path, {"report.csv": b"a,b\n1,2\n", "notes.txt": b"hello"}
    )

    options = _options(
        monkeypatch,
        _config(plugin_dir),
        workspace_path=workspace,
        attachments_path=mount,
    )

    prompt = options.system_prompt or ""
    for name in ("report.csv", "notes.txt"):
        path = mount / name
        assert str(path) in prompt, (
            f"{name} is on disk but its absolute path is not in the system "
            f"prompt. cwd is the workspace, so a bare filename resolves to "
            f"{workspace / name} and the read fails."
        )
        # The path the model is handed must be a real, readable file, not a
        # plausible-looking string assembled from the ref.
        assert path.read_bytes()


def test_the_attachment_preamble_does_not_move_the_working_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The workspace stays the session's cwd. Re-pointing cwd at the attachment
    # mount would make the files reachable by bare name and simultaneously
    # break every relative path into the repository the agent was given.
    plugin_dir = _bundle(tmp_path / "plugin")
    workspace = _workspace(tmp_path)
    mount = _attachments(tmp_path, {"report.csv": b"a,b\n"})

    options = _options(
        monkeypatch,
        _config(plugin_dir),
        workspace_path=workspace,
        attachments_path=mount,
    )

    assert options.cwd == str(workspace)


def test_attachments_arrive_without_a_managed_workspace_too(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The two lanes are independent: a generic (non-workspace) agent can still
    # be sent a file. Coupling the preamble to a mounted checkout would make
    # attachments silently invisible on every deployment without one.
    plugin_dir = _bundle(tmp_path / "plugin")
    mount = _attachments(tmp_path, {"report.csv": b"a,b\n"})

    options = _options(
        monkeypatch, _config(plugin_dir), attachments_path=mount
    )

    assert str(mount / "report.csv") in (options.system_prompt or "")
    assert options.cwd is None


# --- the absent case is byte-identical to today -----------------------------


def _baseline_prompt(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> str | None:
    plugin_dir = _bundle(tmp_path / "plugin")
    return _options(
        monkeypatch, _config(plugin_dir), workspace_path=_workspace(tmp_path)
    ).system_prompt


def test_no_attachments_leaves_the_session_prompt_exactly_as_it_is_today(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The overwhelming majority of turns. An empty-but-present mount is the
    # normal shape of "no files this turn" -- the chart mounts the emptyDir
    # unconditionally -- so it must read as absent, not as a preamble saying
    # nothing arrived.
    baseline = _baseline_prompt(tmp_path / "base", monkeypatch)

    plugin_dir = _bundle(tmp_path / "plugin")
    empty = _attachments(tmp_path, {})
    options = _options(
        monkeypatch,
        _config(plugin_dir),
        workspace_path=_workspace(tmp_path),
        attachments_path=empty,
    )

    assert options.system_prompt == baseline


def test_a_mount_that_never_appeared_is_not_an_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A pod where attachments are switched off has no volume at all. That is
    # ordinary, not a boot failure.
    baseline = _baseline_prompt(tmp_path / "base", monkeypatch)

    plugin_dir = _bundle(tmp_path / "plugin")
    options = _options(
        monkeypatch,
        _config(plugin_dir),
        workspace_path=_workspace(tmp_path),
        attachments_path=tmp_path / "never-mounted",
    )

    assert options.system_prompt == baseline


def test_a_hidden_bookkeeping_entry_alone_does_not_read_as_an_attachment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The init container may leave its own scratch or marker behind. Only files
    # a person actually attached may be announced to the model.
    baseline = _baseline_prompt(tmp_path / "base", monkeypatch)

    plugin_dir = _bundle(tmp_path / "plugin")
    mount = _attachments(tmp_path, {".curie-attachments-stage": b"{}"})
    options = _options(
        monkeypatch,
        _config(plugin_dir),
        workspace_path=_workspace(tmp_path),
        attachments_path=mount,
    )

    assert options.system_prompt == baseline


# --- the process actually passes the mount to build_runner ------------------
#
# ``build_runner`` growing an ``attachments_path`` parameter that ``_serve``
# never fills is the runner-side twin of the kernel wiring defect this stage
# exists to close: every test above would still pass while no real pod ever
# announced a file. So these drive the process entrypoint and read what it
# handed over, with only the HTTP server and the boot fetches isolated.


def _serve_kwargs(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    captured: dict[str, Any] = {}

    config = SimpleNamespace(
        session=SimpleNamespace(session_id="session-2567"),
        model="fake-model",
        port=8080,
        harness="claude",
        runner_token=None,
    )
    monkeypatch.setenv("CURIE_FAKE_MODEL", "1")
    monkeypatch.setattr(RunnerConfig, "from_env", lambda _env: config)
    monkeypatch.setattr(boot, "_resolve_harness", lambda _name: object())

    async def _fake_fetches(
        _config: object, _fake_model: bool, _sdk_env: object
    ) -> Any:
        return boot._BootFetches(  # noqa: SLF001 -- the module's own boot record
            memory_store=object(),  # type: ignore[arg-type]
            memory_preamble=None,
            history_store=object(),  # type: ignore[arg-type]
            conversation_preamble=None,
            mcp_capability=None,
        )

    monkeypatch.setattr(boot, "_load_boot_fetches", _fake_fetches)

    class _Runner:
        _approval_gate = None

        async def start(self) -> None:
            return None

    def _capture(_config: object, **kwargs: Any) -> Any:
        captured.update(kwargs)
        return _Runner()

    monkeypatch.setattr(boot, "build_runner", _capture)
    monkeypatch.setattr(
        boot, "create_app", lambda *_args, **_kwargs: SimpleNamespace(on_startup=[])
    )
    monkeypatch.setattr(web, "run_app", lambda *_args, **_kwargs: None)

    boot._serve()  # noqa: SLF001 -- the process entrypoint is the subject
    return captured


def test_the_process_hands_a_populated_mount_to_build_runner(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    mount = _attachments(tmp_path, {"report.csv": b"a,b\n"})
    monkeypatch.setattr(boot, "ATTACHMENTS_DIR", mount)

    kwargs = _serve_kwargs(monkeypatch)

    assert kwargs.get("attachments_path") == mount, (
        "_serve did not pass attachments_path=; build_runner's parameter is "
        "then unreachable in production and no pod ever announces a file."
    )


def test_the_process_passes_nothing_when_no_volume_was_mounted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    # Discovery is a directory probe, exactly as it is for /workspace: the
    # reference env is scoped away from the runner container, so there is
    # nothing else to read.
    monkeypatch.setattr(boot, "ATTACHMENTS_DIR", tmp_path / "never-mounted")

    kwargs = _serve_kwargs(monkeypatch)

    assert kwargs.get("attachments_path") is None
