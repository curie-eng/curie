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

Subagent model routing can replace a role's ``model:`` without touching the
alias. Observed on CLI 2.1.301 with these role files:
``CLAUDE_CODE_SUBAGENT_MODEL_FORCE=1`` with ``CLAUDE_CODE_SUBAGENT_MODEL=<id>``
runs every role on ``<id>``; ``CLAUDE_CODE_SUBAGENT_MODEL_FORCE=1`` alone makes
each role inherit the implementer's model; ``CLAUDE_CODE_SUBAGENT_MODEL`` alone
changes nothing; and a workspace ``availableModels`` that leaves the role's
model out makes it inherit the implementer's model. Each route has a service
mode control here, then the personal exclusion: env routes are removed (D5),
settings routes refuse before the CLI starts (D2, D3). The workflow sibling
``CLAUDE_CODE_WORKFLOW_SUBAGENT_MODEL`` did not change an ``Agent`` role's model
on CLI 2.1.301, so it is pinned by the classifier, the env readback and the
settings refusals in ``test_personal_claude_auth.py`` instead.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Mapping
from pathlib import Path

import pytest
from aci_protocol import SessionStatus
from curie_runner import personal_auth
from curie_runner.personal_auth import AUTH_MODE_ENV, PersonalLaunchRefusedError
from personal_auth_support import (
    FORCED_SUBAGENT_MODEL,
    IMPLEMENTER_MARKER,
    IMPLEMENTER_MODEL,
    OWNER_GRANT,
    REVIEWER_MODEL,
    SERVICE_API_KEY,
    SERVICE_OAUTH,
    Installation,
    RecordedRequest,
    Reply,
    TextReply,
    ToolCall,
    assert_cli_never_started,
    assert_owner_grant_only,
    connect_session,
    installed,
    requires_cli,
    run_turn,
    wire_session,
    write_bundle,
    write_json,
)

_FACTORY = Path(__file__).resolve().parents[2] / "examples" / "dark-factory"
_ROLES = ("plan-reviewer", "diff-reviewer")
# Ambient defaults no personal session may use.
_AMBIENT_OPUS_DEFAULT = "claude-opus-4-1-20250805"
# The env route that replaces a role's ``model:`` (observed, see the docstring).
_FORCING = {
    "CLAUDE_CODE_SUBAGENT_MODEL_FORCE": "1",
    "CLAUDE_CODE_SUBAGENT_MODEL": FORCED_SUBAGENT_MODEL,
}


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


def _review_bundle(install: Installation) -> tuple[str, _ReviewScript]:
    plugin = _plugin_name()
    bundle = write_bundle(
        install.root / "bundle",
        name=plugin,
        agents={f"{role}.md": _role_text(role) for role in _ROLES},
    )
    script = _ReviewScript(plugin)
    install.proxy.responder = script
    return bundle, script


def _review_turn(
    install: Installation,
    *,
    reviewer_model: str,
    explicit_env: Mapping[str, str] | None = None,
) -> tuple[_ReviewScript, dict[str, list[RecordedRequest]]]:
    """One implementer turn that calls both roles; the model requests by role."""

    bundle, script = _review_bundle(install)
    outcome = run_turn(
        wire_session(
            install,
            reviewer_model=reviewer_model,
            plugin_dir=bundle,
            max_turns=8,
            explicit_env=explicit_env,
        )
    )
    assert outcome.final.status is SessionStatus.DONE, outcome.errors
    by_role: dict[str, list[RecordedRequest]] = {}
    for request in install.proxy.model_requests():
        by_role.setdefault(script.role_of(request), []).append(request)
    assert sorted(by_role) == sorted(["implementer", *_ROLES]), sorted(by_role)
    # Both verdicts came back to the implementer, so each role finished in turn.
    returned = by_role["implementer"][-1].tool_result_text()
    for role in _ROLES:
        assert f"REVIEWER: {role}" in returned, f"the {role} verdict never returned"
    return script, by_role


def _role_models(by_role: Mapping[str, list[RecordedRequest]]) -> dict[str, set[str | None]]:
    return {role: {request.model for request in by_role[role]} for role in ("implementer", *_ROLES)}


def _personal(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(AUTH_MODE_ENV, "personal")
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)


@requires_cli
@pytest.mark.parametrize("reviewer_model", ["claude-opus-5-5", "claude-fable-5-1"])
def test_both_reviewer_roles_use_the_configured_model_and_the_owner_grant(
    install: Installation, monkeypatch: pytest.MonkeyPatch, reviewer_model: str
) -> None:
    _personal(monkeypatch)
    monkeypatch.setenv("ANTHROPIC_DEFAULT_OPUS_MODEL", _AMBIENT_OPUS_DEFAULT)
    monkeypatch.setenv("CURIE_CREDENTIALS", SERVICE_OAUTH)
    monkeypatch.setenv("ANTHROPIC_API_KEY", SERVICE_API_KEY)

    script, by_role = _review_turn(install, reviewer_model=reviewer_model)

    # The CLI names a plugin agent ``<plugin>:<file stem>``, the name the
    # implement-issue skill passes as subagent_type (observed on CLI 2.1.301 in
    # the implementer's agent listing).
    listing = json.dumps(by_role["implementer"][0].body)
    for subagent_type in script.subagent_types:
        assert subagent_type in listing, f"the CLI does not list {subagent_type}"
    for role in _ROLES:
        assert len(by_role[role]) == 1, f"{role} sent {len(by_role[role])} requests"
    assert _role_models(by_role) == {
        "implementer": {IMPLEMENTER_MODEL},
        **{role: {reviewer_model} for role in _ROLES},
    }
    assert_owner_grant_only(install.proxy)


# --- D5: subagent model forcing in the env -------------------------------------------------


@requires_cli
@pytest.mark.parametrize("source", ["inherited", "options-env"])
def test_subagent_model_forcing_reroutes_both_reviewer_roles_in_service_mode(
    install: Installation, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    # The positive control for the case below: mode absent, the same explicit
    # reviewer, so only the forcing differs from the configured route.
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
    explicit: dict[str, str] | None = None
    if source == "inherited":
        for name, value in _FORCING.items():
            monkeypatch.setenv(name, value)
    else:
        explicit = dict(_FORCING)

    _, by_role = _review_turn(install, reviewer_model=REVIEWER_MODEL, explicit_env=explicit)

    assert _role_models(by_role) == {
        "implementer": {IMPLEMENTER_MODEL},
        **{role: {FORCED_SUBAGENT_MODEL} for role in _ROLES},
    }


@requires_cli
@pytest.mark.parametrize("source", ["inherited", "options-env"])
def test_personal_mode_removes_subagent_model_forcing(
    install: Installation, monkeypatch: pytest.MonkeyPatch, source: str
) -> None:
    _personal(monkeypatch)
    explicit: dict[str, str] | None = None
    if source == "inherited":
        for name, value in _FORCING.items():
            monkeypatch.setenv(name, value)
    else:
        explicit = dict(_FORCING)

    _, by_role = _review_turn(install, reviewer_model=REVIEWER_MODEL, explicit_env=explicit)

    assert _role_models(by_role) == {
        "implementer": {IMPLEMENTER_MODEL},
        **{role: {REVIEWER_MODEL} for role in _ROLES},
    }
    assert_owner_grant_only(install.proxy)


# --- D2, D3: workspace settings that reroute a reviewer role ---------------------------------
#
# Project and local settings (``<cwd>/.claude/settings.json`` and
# ``settings.local.json``, ADR 0189) are agent writable. Each case maps the
# settings files to the model the control observes on both roles.

# Each case maps the settings files to the models the control observes: the
# implementer's, then both roles'.
REROUTING_SETTINGS: dict[str, tuple[dict[str, object], str, str]] = {
    "env-subagent-model-force": (
        {"settings.json": {"env": _FORCING}},
        IMPLEMENTER_MODEL,
        FORCED_SUBAGENT_MODEL,
    ),
    "local-env-subagent-model-force": (
        {"settings.local.json": {"env": _FORCING}},
        IMPLEMENTER_MODEL,
        FORCED_SUBAGENT_MODEL,
    ),
    "env-subagent-model-force-alone": (
        {"settings.json": {"env": {"CLAUDE_CODE_SUBAGENT_MODEL_FORCE": "1"}}},
        IMPLEMENTER_MODEL,
        IMPLEMENTER_MODEL,
    ),
    # Observed on CLI 2.1.301: a role whose model is not in the allowlist
    # inherits the parent's model ("Subagent model ... is not in the
    # availableModels allowlist; ... inheriting the parent model instead").
    "available-models": (
        {"settings.json": {"availableModels": [IMPLEMENTER_MODEL]}},
        IMPLEMENTER_MODEL,
        IMPLEMENTER_MODEL,
    ),
    # Observed on CLI 2.1.301 on the first-party endpoint: ``modelOverrides``
    # rewrites the outgoing request model of whichever role resolves to a mapped
    # ID, and an ``availableModels`` that lists both configured models does not
    # stop it. Workspace ``modelOverrides`` was already refused; these cases are
    # also the CLI-side control for the managed ``modelOverrides`` refusals in
    # test_personal_claude_auth.py, since the pinned CLI reads managed policy
    # only at /etc/claude-code.
    "model-overrides-reviewer": (
        {"settings.json": {"modelOverrides": {REVIEWER_MODEL: FORCED_SUBAGENT_MODEL}}},
        IMPLEMENTER_MODEL,
        FORCED_SUBAGENT_MODEL,
    ),
    "model-overrides-reviewer-with-allowlist": (
        {
            "settings.json": {
                "availableModels": [IMPLEMENTER_MODEL, REVIEWER_MODEL],
                "modelOverrides": {REVIEWER_MODEL: FORCED_SUBAGENT_MODEL},
            }
        },
        IMPLEMENTER_MODEL,
        FORCED_SUBAGENT_MODEL,
    ),
    "local-model-overrides-implementer": (
        {"settings.local.json": {"modelOverrides": {IMPLEMENTER_MODEL: FORCED_SUBAGENT_MODEL}}},
        FORCED_SUBAGENT_MODEL,
        REVIEWER_MODEL,
    ),
}


def _plant_workspace(install: Installation, files: Mapping[str, object]) -> None:
    for name, content in files.items():
        write_json(install.workspace / ".claude" / name, content)


@requires_cli
@pytest.mark.parametrize("case", list(REROUTING_SETTINGS))
def test_rerouting_workspace_settings_are_live_in_service_mode(
    install: Installation, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    # The positive control for the refusal below.
    files, implementer, roles = REROUTING_SETTINGS[case]
    monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", OWNER_GRANT)
    _plant_workspace(install, files)

    _, by_role = _review_turn(install, reviewer_model=REVIEWER_MODEL)

    assert _role_models(by_role) == {
        "implementer": {implementer},
        **{role: {roles} for role in _ROLES},
    }


@pytest.mark.parametrize("case", list(REROUTING_SETTINGS))
def test_rerouting_workspace_settings_are_refused_before_the_cli_starts(
    install: Installation, monkeypatch: pytest.MonkeyPatch, case: str
) -> None:
    files, _, _ = REROUTING_SETTINGS[case]
    _personal(monkeypatch)
    _plant_workspace(install, files)
    bundle, _ = _review_bundle(install)
    wiring = wire_session(install, reviewer_model=REVIEWER_MODEL, plugin_dir=bundle, max_turns=8)

    with pytest.raises(PersonalLaunchRefusedError):
        connect_session(wiring)

    assert_cli_never_started(install.proxy)


@requires_cli
def test_workspace_settings_without_a_rerouting_key_still_run_both_reviewers(
    install: Installation, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Liveness for the refusals above: ordinary project and local settings,
    # including a non-provider env name, keep both roles on the reviewer.
    _personal(monkeypatch)
    _plant_workspace(
        install,
        {
            "settings.json": {"permissions": {"allow": ["Read"]}, "env": {"FOO": "bar"}},
            "settings.local.json": {"permissions": {"deny": []}},
        },
    )

    _, by_role = _review_turn(install, reviewer_model=REVIEWER_MODEL)

    assert _role_models(by_role) == {
        "implementer": {IMPLEMENTER_MODEL},
        **{role: {REVIEWER_MODEL} for role in _ROLES},
    }
    assert_owner_grant_only(install.proxy)


@requires_cli
def test_a_managed_allowlist_naming_both_models_keeps_both_roles_on_the_reviewer(
    install: Installation, monkeypatch: pytest.MonkeyPatch, managed_dir: Path
) -> None:
    # Liveness for the managed ``availableModels`` and ``modelOverrides``
    # refusals in test_personal_claude_auth.py (an empty mapping is allowed).
    # The pinned CLI reads managed policy only at /etc/claude-code, so this
    # proves the guard's decision offline; the CLI-side effect of an allowlist
    # or a mapping is the workspace control above.
    _personal(monkeypatch)
    write_json(
        managed_dir / "managed-settings.json",
        {
            "availableModels": [IMPLEMENTER_MODEL, REVIEWER_MODEL, FORCED_SUBAGENT_MODEL],
            "modelOverrides": {},
        },
    )

    _, by_role = _review_turn(install, reviewer_model=REVIEWER_MODEL)

    assert _role_models(by_role) == {
        "implementer": {IMPLEMENTER_MODEL},
        **{role: {REVIEWER_MODEL} for role in _ROLES},
    }
    assert_owner_grant_only(install.proxy)
