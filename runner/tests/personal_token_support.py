"""Synthetic native-channel corpus shared by local and candidate-image tests."""

from __future__ import annotations

import errno
import json
import os
import shlex
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CONTAINMENT_CHANNELS = ("Read", "Bash", "hook", "helper")
CONTAINMENT_PATHS = ("direct", "symlink", "hardlink", "env", "proc_self", "proc_cli", "proc_root")
PUBLIC_MARKER = "PUBLIC-CONTROL-personal-custody"
CONNECTOR_VALUE = "connector-control-$value-permitted"

_PROBE = """\
import errno, json, os, pathlib, sys
spec = json.loads(pathlib.Path(sys.argv[1]).read_text())
result = {"public": pathlib.Path(spec["public"]).read_text()}
if spec["path"] == "env":
    result["observed"] = dict(os.environ)
elif spec["path"] == "hardlink":
    protected = pathlib.Path(spec["link"] + "." + str(os.getpid()))
    public = pathlib.Path(str(protected) + ".public")
    os.link(spec["public"], public)
    result["public_link"] = public.read_text()
    try:
        os.link(spec["target"], protected)
    except OSError as exc:
        if exc.errno not in (errno.EPERM, errno.EACCES, errno.EXDEV):
            raise
        result["link_denied"] = exc.errno
    else:
        raise RuntimeError("protected process hardlink creation succeeded")
else:
    try:
        result["observed"] = pathlib.Path(spec["target"]).read_bytes().decode("utf-8", "replace")
    except OSError as exc:
        if exc.errno not in (errno.EPERM, errno.EACCES):
            raise
        result["read_denied"] = exc.errno
print(json.dumps(result))
"""

_HOOK = """\
import json, pathlib, subprocess, sys
root = pathlib.Path(__file__).parent
spec_path = root / "case.json"
if spec_path.is_file():
    spec = json.loads(spec_path.read_text())
    if spec["channel"] in ("hook", "helper"):
        # Both the command hook and its child exercise the corpus separately.
        output = subprocess.check_output([sys.executable, str(root / "probe.py"), str(spec_path)])
        (root / "helper.raw").write_bytes(output)
        namespace = {"__name__": "__main__", "__file__": str(root / "probe.py")}
        original = sys.argv
        sys.argv = [str(root / "probe.py"), str(spec_path)]
        import contextlib, io
        capture = io.StringIO()
        try:
            with contextlib.redirect_stdout(capture):
                code = compile((root / "probe.py").read_text(), str(root / "probe.py"), "exec")
                exec(code, namespace)
        finally:
            sys.argv = original
        (root / "hook.raw").write_text(capture.getvalue())
print("{}")
"""


@dataclass(frozen=True)
class ContainmentBundle:
    root: Path
    public: Path


def write_containment_bundle(root: Path) -> ContainmentBundle:
    (root / ".claude-plugin").mkdir(parents=True)
    public = root / "public.txt"
    public.write_text(PUBLIC_MARKER, encoding="utf-8")
    (root / "probe.py").write_text(_PROBE, encoding="utf-8")
    (root / "hook.py").write_text(_HOOK, encoding="utf-8")
    (root / ".claude-plugin" / "plugin.json").write_text(
        json.dumps(
            {
                "name": "custody-probe",
                "version": "0.1.0",
                "hooks": {
                    "PreToolUse": [
                        {
                            "matcher": "Read|Bash",
                            "hooks": [
                                {
                                    "type": "command",
                                    "command": shlex.join([sys.executable, str(root / "hook.py")]),
                                }
                            ],
                        }
                    ]
                },
            }
        ),
        encoding="utf-8",
    )
    return ContainmentBundle(root, public)


def native_cli_pid(session: Any) -> int:
    """Inspect the SDK's live process handle without changing production wiring."""

    query = session._client._query
    transport = query._transport if hasattr(query, "_transport") else query.transport
    pid = transport._process.pid
    if not isinstance(pid, int) or pid <= 0 or not Path(f"/proc/{pid}").is_dir():
        raise RuntimeError("the SDK has no running native CLI process")
    return pid


def containment_tool(
    channel: str, path: str, bundle: ContainmentBundle, cli_pid: int
) -> list[tuple[str, dict[str, Any]]]:
    """Prepare real aliases and return the exact native calls for one case."""

    if channel not in CONTAINMENT_CHANNELS or path not in CONTAINMENT_PATHS:
        raise ValueError("unknown containment case")
    target = f"/proc/{cli_pid}/environ"
    if path == "proc_self":
        target = "/proc/self/environ"
    elif path == "proc_root":
        target = f"/proc/{cli_pid}/root/proc/{cli_pid}/environ"
    elif path == "symlink":
        alias = bundle.root / f"{channel}-protected-symlink"
        alias.symlink_to(target)
        target = str(alias)
    spec = {
        "channel": channel,
        "path": path,
        "target": target,
        "public": str(bundle.public),
        "link": str(bundle.root / f"{channel}-protected-link"),
    }
    (bundle.root / "case.json").write_text(json.dumps(spec), encoding="utf-8")
    if channel in ("hook", "helper"):
        return [("Read", {"file_path": str(bundle.public)})]
    if channel == "Bash":
        return [
            (
                "Bash",
                {
                    "command": shlex.join(
                        [
                            sys.executable,
                            str(bundle.root / "probe.py"),
                            str(bundle.root / "case.json"),
                        ]
                    )
                },
            )
        ]
    public = bundle.public
    if path == "hardlink":
        protected_link = Path(spec["link"])
        public_link = Path(str(protected_link) + ".public")
        os.link(bundle.public, public_link)
        try:
            os.link(target, protected_link)
        except OSError as exc:
            if exc.errno not in (errno.EPERM, errno.EACCES, errno.EXDEV):
                raise
            (bundle.root / "Read-link-denied.json").write_text(json.dumps({"errno": exc.errno}))
        else:
            raise RuntimeError("protected process hardlink creation succeeded")
        return [("Read", {"file_path": str(public_link)})]
    return [
        ("Read", {"file_path": target}),
        ("Read", {"file_path": str(public)}),
    ]


def assert_containment_capture(
    channel: str, path: str, bundle: ContainmentBundle, raw: str, sentinel: str
) -> None:
    """Check raw tool and command output before the runner's outbound redactor."""

    recovered_grant = sentinel in raw
    public_observed = PUBLIC_MARKER in raw
    assert not recovered_grant, "native tool recovered the synthetic grant"
    assert public_observed, "native channel did not read the public control"
    if channel in ("hook", "helper"):
        for name in ("hook", "helper"):
            captured = (bundle.root / f"{name}.raw").read_text(encoding="utf-8")
            recovered_grant = sentinel in captured
            assert not recovered_grant, "command subprocess recovered the synthetic grant"
            payload = json.loads(captured)
            assert payload["public"] == PUBLIC_MARKER
            if path == "hardlink":
                assert payload["public_link"] == PUBLIC_MARKER
                assert payload["link_denied"] in (errno.EPERM, errno.EACCES, errno.EXDEV)
    if channel == "Read" and path == "hardlink":
        assert json.loads((bundle.root / "Read-link-denied.json").read_text())["errno"] in (
            errno.EPERM,
            errno.EACCES,
            errno.EXDEV,
        )
    target_missing = "No such file" in raw
    assert not target_missing, "missing protected target is not containment proof"


class ContainmentResponder:
    """Deterministic provider script; tool results are captured before ACI redaction."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.raw_results: list[str] = []
        self._index = 0

    def __call__(self, request: Any) -> Any:
        from personal_auth_support import TextReply, ToolCall

        self.raw_results.append(request.tool_result_text())
        if self._index < len(self.calls):
            name, tool_input = self.calls[self._index]
            self._index += 1
            return ToolCall(name, tool_input, f"toolu_custody_{self._index}")
        return TextReply(PUBLIC_MARKER)
