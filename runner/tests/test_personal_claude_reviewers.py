"""Both factory reviewer roles run on the configured reviewer model (#5373, AC 3).

The dark factory's ``plan-reviewer`` and ``diff-reviewer`` declare ``model:
opus``. The CLI resolves that alias through ``ANTHROPIC_DEFAULT_OPUS_MODEL``
(https://code.claude.com/docs/en/env-vars), which personal mode must set from
the explicit ``reviewer_model`` and from nothing else. These cases build a
bundle from the real role files, read at test time from
``examples/dark-factory/agents/`` and never changed here, and drive the real
bundled CLI through ``SessionRunner``.

The scripted stand-in makes the implementer call each role the way the
``implement-issue`` skill does (``Agent`` with ``subagent_type
dark-factory:<role>`` and ``run_in_background: false``), answers each role with
a verdict, then ends the turn. Each request is attributed to its role by the
role file's own first body line in the request's system text.

Ambient service defaults conflict on purpose: the process env carries a
different ``ANTHROPIC_DEFAULT_OPUS_MODEL``, a service ``CURIE_CREDENTIALS`` and a
service ``ANTHROPIC_API_KEY``. For two different explicit reviewer models, each
role's native request must carry that model, the implementer its own model,
and every request the owner grant alone.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path

import pytest
from aci_protocol import SessionStatus
from curie_runner import personal_auth
from curie_runner.personal_auth import AUTH_MODE_ENV
from personal_auth_support import (
    IMPLEMENTER_MARKER,
    IMPLEMENTER_MODEL,
    OWNER_GRANT,
    SERVICE_API_KEY,
    SERVICE_OAUTH,
    Installation,
    RecordedRequest,
    Reply,
    TextReply,
    ToolCall,
    assert_owner_grant_only,
    installed,
    requires_cli,
    run_turn,
    wire_session,
    write_bundle,
)

_FACTORY = Path(__file__).resolve().parents[2] / "examples" / "dark-factory"
_ROLES = ("plan-reviewer", "diff-reviewer")
# Ambient defaults no personal session may use.
_AMBIENT_OPUS_DEFAULT = "claude-opus-4-1-20250805"


@pytest.fixture(autouse=True)
def managed_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An empty stand-in for ``/etc/claude-code``.

    Tests cannot write ``/etc/claude-code``, and a host policy there must not
    decide these cases. ``test_personal_claude_live.py`` proves the real path.
    """

    path = tmp_path / "managed"
    path.mkdir()
    monkeypatch.setattr(personal_auth, "MANAGED_SETTINGS_DIR", path)
    return path


@pytest.fixture
def install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Installation]:
    with installed(tmp_path, monkeypatch) as installation:
        yield installation


def _role_text(role: str) -> str:
    return (_FACTORY / "agents" / f"{role}.md").read_text(encoding="utf-8")


def _first_body_line(text: str) -> str:
    """The first line after the role file's frontmatter."""

    _, _, body = text.split("---", 2)
    return next(line for line in body.splitlines() if line.strip())


def _plugin_name() -> str:
    manifest = json.loads((_FACTORY / ".claude-plugin" / "plugin.json").read_text("utf-8"))
    name = manifest["name"]
    assert isinstance(name, str)
    return name


class _ReviewScript:
    """The implementer calls each role in turn, each role approves, then the turn ends."""

    def __init__(self, plugin: str) -> None:
        self.subagent_types = [f"{plugin}:{role}" for role in _ROLES]
        self.role_lines = {role: _first_body_line(_role_text(role)) for role in _ROLES}

    def role_of(self, request: RecordedRequest) -> str:
        if IMPLEMENTER_MARKER in request.system:
            return "implementer"
        for role, line in self.role_lines.items():
            if line in request.system:
                return role
        return "unattributed"

    def __call__(self, request: RecordedRequest) -> Reply:
        role = self.role_of(request)
        if role == "implementer":
            done = len(request.tool_results())
            if done < len(self.subagent_types):
                subagent_type = self.subagent_types[done]
                return ToolCall(
                    "Agent",
                    {
                        "subagent_type": subagent_type,
                        "description": f"Review round {done + 1}",
                        "prompt": "Review the change for issue 1.",
                        "run_in_background": False,
                    },
                    f"toolu_review_{done + 1}",
                )
            return TextReply("done")
        if role in _ROLES:
            return TextReply(f"REVIEWER: {role}\nVERDICT: approve")
        return TextReply("done")


@requires_cli
@pytest.mark.parametrize("reviewer_model", ["claude-opus-5-5", "claude-fable-5-1"])
def test_both_reviewer_roles_use_the_configured_model_and_the_owner_grant(
    install: Installation, monkeypatch: pytest.MonkeyPatch, reviewer_model: str
) -> None:
    plugin = _plugin_name()
    bundle = write_bundle(
        install.root / "bundle",
        name=plugin,
        agents={f"{role}.md": _role_text(role) for role in _ROLES},
    )
    script = _ReviewScript(plugin)
    install.proxy.responder = script
    monkeypatch.setenv(AUTH_MODE_ENV, "personal")
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
    monkeypatch.setenv("ANTHROPIC_DEFAULT_OPUS_MODEL", _AMBIENT_OPUS_DEFAULT)
    monkeypatch.setenv("CURIE_CREDENTIALS", SERVICE_OAUTH)
    monkeypatch.setenv("ANTHROPIC_API_KEY", SERVICE_API_KEY)

    outcome = run_turn(
        wire_session(install, reviewer_model=reviewer_model, plugin_dir=bundle, max_turns=8)
    )

    assert outcome.final.status is SessionStatus.DONE, outcome.errors
    requests = install.proxy.model_requests()
    by_role: dict[str, list[RecordedRequest]] = {}
    for request in requests:
        by_role.setdefault(script.role_of(request), []).append(request)
    assert sorted(by_role) == sorted(["implementer", *_ROLES]), sorted(by_role)
    # The CLI names a plugin agent ``<plugin>:<file stem>``, the name the
    # implement-issue skill passes as subagent_type (observed on CLI 2.1.301 in
    # the implementer's agent listing).
    listing = json.dumps(by_role["implementer"][0].body)
    for subagent_type in script.subagent_types:
        assert subagent_type in listing, f"the CLI does not list {subagent_type}"
    assert {request.model for request in by_role["implementer"]} == {IMPLEMENTER_MODEL}
    for role in _ROLES:
        assert len(by_role[role]) == 1, f"{role} sent {len(by_role[role])} requests"
        [request] = by_role[role]
        assert request.model == reviewer_model, f"{role} ran on {request.model}"
    # Both verdicts came back to the implementer, so each role finished in turn.
    returned = by_role["implementer"][-1].tool_result_text()
    for role in _ROLES:
        assert f"REVIEWER: {role}" in returned, f"the {role} verdict never returned"
    assert_owner_grant_only(install.proxy)
