"""Personal Claude authentication through the real runner path (#5373, ADR 0268 rows 1 and 3).

``CURIE_MODEL_AUTH_MODE=personal`` makes the owner's ``CLAUDE_CODE_OAUTH_TOKEN``
the only model grant. Every success case here goes through
``resolve_sdk_env(os.environ)``, ``build_options``, ``ClaudeAgentSession.connect``
and ``SessionRunner`` into the real bundled CLI, which reaches the model only
through the installation proxy in ``personal_auth_support``. What the CLI sent
is read off the wire, not off an options dict.

AC 1, competing inputs: one case per input class (cloud selectors, API key,
auth token and custom headers, ``apiKeyHelper``, both base URL paths with the
service credential and a ``CURIE_MODEL_ENV_KEY`` target, profile and
federation state, saved login and gateway state) and one case with all of them
at once. Each personal case must reach ``api.anthropic.com/v1/messages`` with
``Authorization: Bearer <owner grant>``, no ``x-api-key`` and no request to an
alternate host. Each class also runs in service mode with the same inputs; that
control shows the input is live in this CLI, so the personal case is a real
exclusion and not an input the CLI ignores anyway. Mandatory managed policy
that forces gateway authentication, and workspace settings that add a
competing source, are refused before the CLI starts.

AC 2, refusals: a missing, empty or blank grant with every competing source
still present, an unknown mode, and a missing or non-Claude reviewer model each
raise their typed error before the CLI starts (zero CONNECTs). Every refusal
has a liveness case where valid input launches and the turn succeeds, and the
service mode keeps today's behavior.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
from aci_protocol import SessionStatus
from curie_runner import personal_auth
from curie_runner.adapter import build_options
from curie_runner.personal_auth import (
    AUTH_MODE_ENV,
    MANAGED_SETTINGS_DIR,
    AuthenticationRequiredError,
    AuthMode,
    InvalidAuthModeError,
    PersonalAuthError,
    PersonalLaunchRefusedError,
    ReviewerModelRequiredError,
    is_competing_auth_key,
    resolve_auth_mode,
)
from curie_runner.sdk_auth import resolve_sdk_env
from personal_auth_support import (
    ALT_BASE_HOST,
    ALT_BEDROCK_HOST,
    ALT_FOUNDRY_HOST,
    ALT_GATEWAY_HOST,
    ALT_MODEL_BASE_HOST,
    ALT_PROFILE_HOST,
    ALT_VERTEX_HOST,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET,
    BEDROCK_BEARER,
    CUSTOM_HEADER,
    ENV_KEY_TARGET,
    FIRST_PARTY_HOST,
    FOUNDRY_KEY,
    GATEWAY_TOKEN,
    IMPLEMENTER_MARKER,
    OWNER_GRANT,
    PLACEHOLDER_NAMES,
    REVIEWER_MODEL,
    SAVED_API_KEY,
    SAVED_LOGIN,
    SERVICE_API_KEY,
    SERVICE_AUTH_TOKEN,
    SERVICE_OAUTH,
    Installation,
    RecordedRequest,
    Reply,
    TextReply,
    ToolCall,
    TurnOutcome,
    assert_cli_never_started,
    assert_owner_grant_only,
    connect_session,
    installed,
    merge_json,
    requires_cli,
    run_turn,
    wire_session,
    write_api_key_helper,
    write_json,
)

GRANT_ENV = "CLAUDE_CODE_OAUTH_TOKEN"
REVIEWER_ENV = "ANTHROPIC_DEFAULT_OPUS_MODEL"
# A provider-native credential name a config author may declare (#514).
ENV_KEY_NAME = "ACME_MODEL_CREDENTIAL"


@pytest.fixture(autouse=True)
def managed_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """An empty stand-in for ``/etc/claude-code``.

    Tests cannot write ``/etc/claude-code``, and a host policy there must not
    decide these cases, so the guard's ``MANAGED_SETTINGS_DIR`` points here.
    The guard reads it at call time. ``test_personal_claude_live.py`` proves the
    real path on a throwaway cluster with a mounted managed settings file.
    """

    path = tmp_path / "managed"
    path.mkdir()
    monkeypatch.setattr(personal_auth, "MANAGED_SETTINGS_DIR", path)
    return path


@pytest.fixture
def install(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[Installation]:
    with installed(tmp_path, monkeypatch) as installation:
        yield installation


def personal(monkeypatch: pytest.MonkeyPatch, grant: str | None = OWNER_GRANT) -> None:
    monkeypatch.setenv(AUTH_MODE_ENV, "personal")
    if grant is not None:
        monkeypatch.setenv(GRANT_ENV, grant)


def setenv_all(monkeypatch: pytest.MonkeyPatch, values: dict[str, str]) -> None:
    for key, value in values.items():
        monkeypatch.setenv(key, value)


def no_placeholder_in(text: str) -> list[str]:
    """Names of placeholder credentials whose value appears in ``text``."""

    return sorted(name for value, name in PLACEHOLDER_NAMES.items() if value in text)


# --- the competing input classes ------------------------------------------------------


def inherited_claude_config(installation: Installation) -> Path:
    return installation.root / "inherited-claude-config"


def inherited_anthropic_config(installation: Installation) -> Path:
    return installation.root / "inherited-anthropic-config"


def helper_marker(installation: Installation) -> Path:
    return installation.root / "api-key-helper.ran"


def plant_bedrock(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    setenv_all(
        monkeypatch,
        {
            "CLAUDE_CODE_USE_BEDROCK": "1",
            "AWS_REGION": "us-east-1",
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID,
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET,
            "AWS_BEARER_TOKEN_BEDROCK": BEDROCK_BEARER,
            "ANTHROPIC_BEDROCK_BASE_URL": f"https://{ALT_BEDROCK_HOST}",
            "CLAUDE_CODE_SKIP_BEDROCK_AUTH": "1",
        },
    )


def plant_vertex(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    service_account = write_json(
        installation.root / "gcp-service-account.json",
        {"type": "service_account", "project_id": "example-project"},
    )
    setenv_all(
        monkeypatch,
        {
            "CLAUDE_CODE_USE_VERTEX": "1",
            "CLOUD_ML_REGION": "us-east5",
            "ANTHROPIC_VERTEX_PROJECT_ID": "example-project",
            "GOOGLE_CLOUD_PROJECT": "example-project",
            "GOOGLE_APPLICATION_CREDENTIALS": str(service_account),
            "ANTHROPIC_VERTEX_BASE_URL": f"https://{ALT_VERTEX_HOST}/v1",
            "CLAUDE_CODE_SKIP_VERTEX_AUTH": "1",
        },
    )


def plant_foundry(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    setenv_all(
        monkeypatch,
        {
            "CLAUDE_CODE_USE_FOUNDRY": "1",
            "ANTHROPIC_FOUNDRY_API_KEY": FOUNDRY_KEY,
            "ANTHROPIC_FOUNDRY_BASE_URL": f"https://{ALT_FOUNDRY_HOST}/anthropic",
        },
    )


def plant_api_key(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ANTHROPIC_API_KEY", SERVICE_API_KEY)


def plant_auth_token(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    setenv_all(
        monkeypatch,
        {
            "ANTHROPIC_AUTH_TOKEN": SERVICE_AUTH_TOKEN,
            "ANTHROPIC_CUSTOM_HEADERS": f"X-Curie-Probe: {CUSTOM_HEADER}",
        },
    )


def plant_api_key_helper(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    helper, marker = write_api_key_helper(installation.root)
    assert marker == helper_marker(installation)
    # User settings in the inherited CLAUDE_CONFIG_DIR and in the default
    # ``~/.claude`` (docs: https://code.claude.com/docs/en/authentication,
    # "Custom credential scripts"; settings files per
    # https://code.claude.com/docs/en/env-vars ``CLAUDE_CONFIG_DIR``).
    for config in (inherited_claude_config(installation), installation.home / ".claude"):
        merge_json(config / "settings.json", {"apiKeyHelper": str(helper)})
    monkeypatch.setenv("CLAUDE_CONFIG_DIR", str(inherited_claude_config(installation)))


def plant_base_url(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    setenv_all(
        monkeypatch,
        {
            "ANTHROPIC_BASE_URL": f"https://{ALT_BASE_HOST}",
            "CURIE_CREDENTIALS": SERVICE_API_KEY,
        },
    )


def plant_model_base_url(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    setenv_all(
        monkeypatch,
        {
            "CURIE_MODEL_BASE_URL": f"https://{ALT_MODEL_BASE_HOST}",
            "CURIE_MODEL_ENV_KEY": f'["{ENV_KEY_NAME}"]',
            ENV_KEY_NAME: ENV_KEY_TARGET,
        },
    )


def plant_profile(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    # Profile file format, configuration directory and active profile:
    # https://platform.claude.com/docs/en/manage-claude/wif-reference
    # ("Profile configuration file"). Planted in the inherited
    # ANTHROPIC_CONFIG_DIR and in the default ``~/.config/anthropic``.
    identity = installation.root / "identity.jwt"
    identity.write_text("eyJhbGciOiJub25lIn0.eyJzdWIiOiJleGFtcGxlIn0.c2ln", encoding="utf-8")
    profile = {
        "version": "1.0",
        "authentication": {
            "type": "oidc_federation",
            "federation_rule_id": "fdrl_PLACEHOLDER",
            "service_account_id": "svac_PLACEHOLDER",
            "identity_token": {"source": "file", "path": str(identity)},
        },
        "organization_id": "00000000-0000-0000-0000-000000000000",
        "base_url": f"https://{ALT_PROFILE_HOST}",
    }
    for config in (
        inherited_anthropic_config(installation),
        installation.home / ".config" / "anthropic",
    ):
        write_json(config / "configs" / "alt.json", profile)
        (config / "active_config").write_text("alt", encoding="utf-8")
    setenv_all(
        monkeypatch,
        {
            "ANTHROPIC_CONFIG_DIR": str(inherited_anthropic_config(installation)),
            "ANTHROPIC_PROFILE": "alt",
            "ANTHROPIC_FEDERATION_RULE_ID": "fdrl_PLACEHOLDER",
            "ANTHROPIC_ORGANIZATION_ID": "00000000-0000-0000-0000-000000000000",
            "ANTHROPIC_SERVICE_ACCOUNT_ID": "svac_PLACEHOLDER",
            "ANTHROPIC_IDENTITY_TOKEN_FILE": str(identity),
        },
    )


def plant_saved_state(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    # Saved login storage on Linux is ``<config dir>/.credentials.json``:
    # https://code.claude.com/docs/en/authentication ("Credential management").
    # The gateway session and ``forceLoginGatewayUrl``: same page,
    # "Authentication precedence". Planted in the inherited CLAUDE_CONFIG_DIR and
    # in the default ``~/.claude`` / ``~/.claude.json``.
    expires = 4_102_444_800_000  # 2100-01-01, in ms
    credentials = {
        "claudeAiOauth": {
            "accessToken": SAVED_LOGIN,
            "refreshToken": "sk-ant-ort01-PLACEHOLDER-refresh",
            "expiresAt": expires,
            "scopes": ["user:inference", "user:profile"],
            "subscriptionType": "max",
        },
        "enterpriseGateway": {
            "url": f"https://{ALT_GATEWAY_HOST}",
            "jwt": "eyJhbGciOiJub25lIn0.eyJzdWIiOiJleGFtcGxlIn0.c2ln",
            "expiresAt": expires,
        },
    }
    config = inherited_claude_config(installation)
    for credentials_path, global_config in (
        (config / ".credentials.json", config / ".claude.json"),
        (
            installation.home / ".claude" / ".credentials.json",
            installation.home / ".claude.json",
        ),
    ):
        write_json(credentials_path, credentials).chmod(0o600)
        merge_json(global_config, {"primaryApiKey": SAVED_API_KEY})
        merge_json(
            credentials_path.parent / "settings.json",
            {"forceLoginGatewayUrl": f"https://{ALT_GATEWAY_HOST}"},
        )
    setenv_all(
        monkeypatch,
        {
            "CLAUDE_CONFIG_DIR": str(config),
            "CLAUDE_CODE_USE_GATEWAY": "1",
            "CLAUDE_CODE_GATEWAY_TOKEN": GATEWAY_TOKEN,
        },
    )


# --- what each service control observes ---------------------------------------------


def model_requests_on(installation: Installation, host: str) -> list[RecordedRequest]:
    return [request for request in installation.proxy.model_requests() if request.host == host]


def control_reaches(host: str) -> Callable[[Installation], None]:
    def check(installation: Installation) -> None:
        assert host in installation.proxy.connect_hosts(), f"no CONNECT to {host}"
        assert any(request.host == host for request in installation.proxy.all_requests()), (
            f"no request reached {host}"
        )

    return check


def control_foundry(installation: Installation) -> None:
    [request, *_] = model_requests_on(installation, ALT_FOUNDRY_HOST)
    assert request.api_key == "foundry key", request.api_key


def control_api_key(installation: Installation) -> None:
    # Docs (authentication, "Authentication precedence"): ANTHROPIC_API_KEY,
    # rank 3, beats CLAUDE_CODE_OAUTH_TOKEN, rank 5, and goes as X-Api-Key.
    [request, *_] = model_requests_on(installation, FIRST_PARTY_HOST)
    assert request.api_key == "service api key", request.api_key
    assert request.bearer is None, request.bearer


def control_auth_token(installation: Installation) -> None:
    # Docs: ANTHROPIC_AUTH_TOKEN, rank 2, goes as Authorization: Bearer.
    # ANTHROPIC_CUSTOM_HEADERS (env-vars docs) adds the probe header.
    [request, *_] = model_requests_on(installation, FIRST_PARTY_HOST)
    assert request.bearer == "service auth token", request.bearer
    assert "custom header" in request.credentials_seen, sorted(request.credentials_seen)


def control_api_key_helper(installation: Installation) -> None:
    # Docs: apiKeyHelper, rank 4, beats CLAUDE_CODE_OAUTH_TOKEN, rank 5.
    # Observed on CLI 2.1.301: the helper output goes as x-api-key.
    assert helper_marker(installation).is_file(), "the apiKeyHelper never ran"
    [request, *_] = model_requests_on(installation, FIRST_PARTY_HOST)
    assert request.api_key == "apiKeyHelper key", request.api_key


def control_base_url(installation: Installation) -> None:
    # resolve_sdk_env's base URL override forwards the provider credential as
    # x-api-key (#252).
    [request, *_] = model_requests_on(installation, ALT_BASE_HOST)
    assert request.api_key == "service api key", request.api_key


def control_model_base_url(installation: Installation) -> None:
    [request, *_] = model_requests_on(installation, ALT_MODEL_BASE_HOST)
    assert request.api_key == "CURIE_MODEL_ENV_KEY target", request.api_key


def control_profile(installation: Installation) -> None:
    # Observed on CLI 2.1.301: a named oidc_federation profile exchanges its
    # identity token at the profile's base_url before any model request.
    exchanges = [
        request
        for request in installation.proxy.all_requests()
        if request.host == ALT_PROFILE_HOST and request.path.startswith("/v1/oauth/token")
    ]
    assert exchanges, "the profile's token exchange never reached its base_url"


def control_saved_state(installation: Installation) -> None:
    # Observed on CLI 2.1.301: the saved claude.ai login authenticates the model
    # request, and the saved ``primaryApiKey`` rides a settings request. The
    # saved gateway session and CLAUDE_CODE_USE_GATEWAY produced no traffic to
    # the gateway host offline, so their exclusion rests on the docs
    # (authentication, "Authentication precedence": a signed-in gateway session
    # outranks every other source) and on the zero-alternate assertion.
    [request, *_] = model_requests_on(installation, FIRST_PARTY_HOST)
    assert request.bearer == "saved claude.ai login", request.bearer
    assert any(
        request.api_key == "saved api key" for request in installation.proxy.all_requests()
    ), "the saved primaryApiKey was never sent"


@dataclass(frozen=True)
class InputClass:
    plant: Callable[[Installation, pytest.MonkeyPatch], None]
    control: Callable[[Installation], None]
    # Docs (authentication, "Authentication precedence"): CLAUDE_CODE_OAUTH_TOKEN
    # (rank 5) outranks profiles (rank 6) and the saved /login (rank 7), so the
    # control for those drops the grant to show the source is live.
    control_drops_grant: bool = False
    personal_extra: Callable[[Installation], None] | None = None


def helper_never_ran(installation: Installation) -> None:
    assert not helper_marker(installation).exists(), "the apiKeyHelper ran in personal mode"


INPUT_CLASSES: dict[str, InputClass] = {
    "bedrock": InputClass(plant_bedrock, control_reaches(ALT_BEDROCK_HOST)),
    "vertex": InputClass(plant_vertex, control_reaches(ALT_VERTEX_HOST)),
    "foundry": InputClass(plant_foundry, control_foundry),
    "api-key": InputClass(plant_api_key, control_api_key),
    "auth-token-and-custom-headers": InputClass(plant_auth_token, control_auth_token),
    "api-key-helper": InputClass(
        plant_api_key_helper, control_api_key_helper, personal_extra=helper_never_ran
    ),
    "anthropic-base-url-and-curie-credentials": InputClass(plant_base_url, control_base_url),
    "curie-model-base-url-and-env-key-target": InputClass(
        plant_model_base_url, control_model_base_url
    ),
    "profile-and-federation": InputClass(plant_profile, control_profile, control_drops_grant=True),
    "saved-login-and-gateway-session": InputClass(
        plant_saved_state, control_saved_state, control_drops_grant=True
    ),
}


def plant_everything(installation: Installation, monkeypatch: pytest.MonkeyPatch) -> None:
    for input_class in INPUT_CLASSES.values():
        input_class.plant(installation, monkeypatch)


# --- AC 1: the owner grant alone reaches the model ----------------------------------------


@requires_cli
@pytest.mark.parametrize("grant_source", ["process-env", "options-env"])
def test_a_personal_turn_sends_only_the_owner_grant_to_first_party_messages(
    install: Installation, monkeypatch: pytest.MonkeyPatch, grant_source: str
) -> None:
    personal(monkeypatch, grant=OWNER_GRANT if grant_source == "process-env" else None)
    explicit = {GRANT_ENV: OWNER_GRANT} if grant_source == "options-env" else None

    outcome = run_turn(wire_session(install, reviewer_model=REVIEWER_MODEL, explicit_env=explicit))

    assert outcome.final.status is SessionStatus.DONE, outcome.errors
    assert_owner_grant_only(install.proxy)


@requires_cli
@pytest.mark.parametrize("input_class", list(INPUT_CLASSES))
def test_a_competing_input_is_excluded_from_the_personal_request(
    install: Installation, monkeypatch: pytest.MonkeyPatch, input_class: str
) -> None:
    case = INPUT_CLASSES[input_class]
    case.plant(install, monkeypatch)
    personal(monkeypatch)

    outcome = run_turn(wire_session(install, reviewer_model=REVIEWER_MODEL))

    assert outcome.final.status is SessionStatus.DONE, outcome.errors
    assert_owner_grant_only(install.proxy)
    if case.personal_extra is not None:
        case.personal_extra(install)


@requires_cli
@pytest.mark.parametrize("input_class", list(INPUT_CLASSES))
def test_the_same_input_is_live_in_service_mode(
    install: Installation, monkeypatch: pytest.MonkeyPatch, input_class: str
) -> None:
    # The positive control for the case above: same inputs, mode absent, so the
    # exclusion is a real one and not an input this CLI ignores anyway.
    case = INPUT_CLASSES[input_class]
    case.plant(install, monkeypatch)
    if not case.control_drops_grant:
        monkeypatch.setenv(GRANT_ENV, OWNER_GRANT)

    run_turn(wire_session(install))

    case.control(install)


_CONFIG_DIRS = ("CLAUDE_CONFIG_DIR", "ANTHROPIC_CONFIG_DIR")
# For each config dir the CLI runs with: its path, its mode, then its entries.
_SHOW_CONFIG = "; ".join(
    f'printf \'{name}=%s\\n\' "${name}"; stat -c \'MODE=%a\' "${name}"; ls -A1 "${name}"'
    for name in _CONFIG_DIRS
)


def _config_listing(text: str, name: str) -> tuple[str, list[str]]:
    """The ``<name>=<dir>`` line the scripted Bash call printed and what followed it."""

    lines = text.splitlines()
    start = next(index for index, line in enumerate(lines) if line.startswith(f"{name}="))
    listing: list[str] = []
    for line in lines[start + 1 :]:
        if line.partition("=")[0] in _CONFIG_DIRS:
            break
        listing.append(line.strip())
    return lines[start].partition("=")[2], listing


def _show_config_then_finish(request: RecordedRequest) -> Reply:
    if IMPLEMENTER_MARKER in request.system and not request.tool_results():
        return ToolCall("Bash", {"command": _SHOW_CONFIG, "description": "Show config"}, "toolu_1")
    return TextReply("done")


@requires_cli
def test_the_combined_conflict_case_uses_the_owner_grant_and_a_fresh_config(
    install: Installation, monkeypatch: pytest.MonkeyPatch
) -> None:
    plant_everything(install, monkeypatch)
    personal(monkeypatch)
    install.proxy.responder = _show_config_then_finish
    # Competing values in the explicit SDK env too: a removal there must not let
    # the inherited snapshot restore a value, nor the other way round.
    explicit = {
        "ANTHROPIC_API_KEY": SERVICE_API_KEY,
        "ANTHROPIC_BASE_URL": f"https://{ALT_BASE_HOST}",
        "CLAUDE_CODE_USE_BEDROCK": "1",
        "CURIE_CREDENTIALS": SERVICE_OAUTH,
        "CLAUDE_CONFIG_DIR": str(inherited_claude_config(install)),
    }

    outcome = run_turn(wire_session(install, reviewer_model=REVIEWER_MODEL, explicit_env=explicit))

    assert outcome.final.status is SessionStatus.DONE, outcome.errors
    assert_owner_grant_only(install.proxy)
    helper_never_ran(install)
    [follow_up] = [
        request
        for request in install.proxy.model_requests()
        if IMPLEMENTER_MARKER in request.system and request.tool_results()
    ]
    shown = follow_up.tool_result_text()
    claude_dir, claude_listing = _config_listing(shown, "CLAUDE_CONFIG_DIR")
    anthropic_dir, anthropic_listing = _config_listing(shown, "ANTHROPIC_CONFIG_DIR")
    # The CLI's config dir is runner-owned and fresh: not the inherited one, not
    # ``~/.claude``, mode 0700, and holding no saved login or user settings.
    assert claude_dir and Path(claude_dir) not in {
        inherited_claude_config(install),
        install.home / ".claude",
    }
    assert claude_listing[0] == "MODE=700", claude_listing[0]
    assert ".credentials.json" not in claude_listing, claude_listing
    assert "settings.json" not in claude_listing, claude_listing
    assert anthropic_dir and Path(anthropic_dir) not in {
        inherited_anthropic_config(install),
        install.home / ".config" / "anthropic",
    }
    assert anthropic_listing[0] == "MODE=700", anthropic_listing[0]
    assert not {"configs", "credentials", "active_config"} & set(anthropic_listing)
    # close() removes the directory it created.
    assert not Path(claude_dir).exists(), "the runner left its CLI config dir behind"


# --- AC 1: mandatory managed policy is refused, never bypassed -------------------------------
#
# Linux managed settings: ``/etc/claude-code/managed-settings.json``, then every
# ``managed-settings.d/*.json`` in alphabetical order; hidden files and files not
# ending in ``.json`` are ignored; a file that is not a JSON object stops the CLI.
# Docs: https://code.claude.com/docs/en/managed-settings. ``forceLoginMethod:
# gateway`` and ``forceLoginGatewayUrl`` make the gateway sign-in the only
# credential: https://code.claude.com/docs/en/authentication ("Authentication
# precedence").

_GATEWAY_POLICY = {
    "forceLoginMethod": "gateway",
    "forceLoginGatewayUrl": f"https://{ALT_GATEWAY_HOST}",
}
MANAGED_REFUSALS: dict[str, dict[str, object]] = {
    "forced-gateway": {"managed-settings.json": _GATEWAY_POLICY},
    "force-login-console": {"managed-settings.json": {"forceLoginMethod": "console"}},
    "force-login-unclassifiable": {"managed-settings.json": {"forceLoginMethod": "sso"}},
    "gateway-url-alone": {
        "managed-settings.json": {"forceLoginGatewayUrl": f"https://{ALT_GATEWAY_HOST}"}
    },
    "drop-in-forces-gateway": {
        "managed-settings.json": {"forceLoginMethod": "claudeai"},
        "managed-settings.d/50-gateway.json": {"forceLoginMethod": "gateway"},
    },
    "policy-helper": {"managed-settings.json": {"policyHelper": "/usr/local/bin/policy"}},
    "api-key-helper": {"managed-settings.json": {"apiKeyHelper": "/usr/local/bin/key"}},
    "aws-auth-refresh": {"managed-settings.json": {"awsAuthRefresh": "aws sso login"}},
    "gcp-auth-refresh": {"managed-settings.json": {"gcpAuthRefresh": "gcloud auth login"}},
    "env-base-url": {
        "managed-settings.json": {"env": {"ANTHROPIC_BASE_URL": f"https://{ALT_BASE_HOST}"}}
    },
    "env-bedrock-selector": {"managed-settings.json": {"env": {"CLAUDE_CODE_USE_BEDROCK": "1"}}},
    "env-oauth-token": {"managed-settings.json": {"env": {GRANT_ENV: SERVICE_OAUTH}}},
    "env-not-an-object": {"managed-settings.json": {"env": ["ANTHROPIC_API_KEY"]}},
    "invalid-json": {"managed-settings.json": "{not json"},
    "not-an-object": {"managed-settings.json": '["forceLoginMethod"]'},
    "drop-in-invalid-json": {"managed-settings.d/10-broken.json": "{not json"},
}


def plant_managed(managed: Path, files: dict[str, object]) -> None:
    for relative, content in files.items():
        target = managed / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, str):
            target.write_text(content, encoding="utf-8")
        else:
            write_json(target, content)


@pytest.mark.parametrize("policy", list(MANAGED_REFUSALS))
def test_managed_policy_with_a_competing_source_is_refused_before_the_cli_starts(
    install: Installation,
    monkeypatch: pytest.MonkeyPatch,
    managed_dir: Path,
    policy: str,
) -> None:
    plant_everything(install, monkeypatch)
    personal(monkeypatch)
    plant_managed(managed_dir, MANAGED_REFUSALS[policy])
    wiring = wire_session(install, reviewer_model=REVIEWER_MODEL)

    with pytest.raises(PersonalLaunchRefusedError) as refused:
        connect_session(wiring)

    assert no_placeholder_in(str(refused.value)) == []
    assert_cli_never_started(install.proxy)


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a mode 000 file")
@pytest.mark.parametrize("unreadable", ["file", "drop-in-dir", "drop-in-file"])
def test_unreadable_managed_policy_is_refused_before_the_cli_starts(
    install: Installation,
    monkeypatch: pytest.MonkeyPatch,
    managed_dir: Path,
    unreadable: str,
) -> None:
    personal(monkeypatch)
    plant_managed(
        managed_dir,
        {
            "managed-settings.json": {"forceLoginMethod": "claudeai"},
            "managed-settings.d/10-ok.json": {"cleanupPeriodDays": 30},
        },
    )
    target = {
        "file": managed_dir / "managed-settings.json",
        "drop-in-dir": managed_dir / "managed-settings.d",
        "drop-in-file": managed_dir / "managed-settings.d" / "10-ok.json",
    }[unreadable]
    target.chmod(0o000)
    try:
        wiring = wire_session(install, reviewer_model=REVIEWER_MODEL)
        with pytest.raises(PersonalLaunchRefusedError):
            connect_session(wiring)
    finally:
        target.chmod(0o700)

    assert_cli_never_started(install.proxy)


@requires_cli
def test_managed_claudeai_policy_with_installation_transport_still_launches(
    install: Installation, monkeypatch: pytest.MonkeyPatch, managed_dir: Path
) -> None:
    # Liveness for every managed refusal above. Managed proxy and CA keys are
    # installation transport; hidden drop-ins and non-.json files are not read by
    # the CLI (managed-settings docs), so their forcing keys do not refuse either.
    personal(monkeypatch)
    plant_managed(
        managed_dir,
        {
            "managed-settings.json": {
                "forceLoginMethod": "claudeai",
                "env": {
                    "HTTPS_PROXY": install.proxy.url,
                    "NODE_EXTRA_CA_CERTS": os.environ["NODE_EXTRA_CA_CERTS"],
                },
            },
            "managed-settings.d/10-retention.json": {"cleanupPeriodDays": 30},
            "managed-settings.d/.hidden.json": _GATEWAY_POLICY,
            "managed-settings.d/notes.txt": '{"forceLoginMethod": "gateway"}',
        },
    )

    outcome = run_turn(wire_session(install, reviewer_model=REVIEWER_MODEL))

    assert outcome.final.status is SessionStatus.DONE, outcome.errors
    assert_owner_grant_only(install.proxy)


def test_the_guard_reads_the_linux_managed_settings_path() -> None:
    # Text pin (#5373): the real ``/etc/claude-code`` cannot be written by an
    # offline test; the live cluster test mounts a policy there and observes the
    # refusal. Docs: https://code.claude.com/docs/en/managed-settings (Linux).
    assert MANAGED_SETTINGS_DIR == Path("/etc/claude-code")


# --- AC 1: workspace settings that add a competing source are refused ------------------------
#
# Project settings are ``<cwd>/.claude/settings.json`` and
# ``<cwd>/.claude/settings.local.json`` (setting sources "project" and "local",
# kept by ADR 0189). A workspace is agent writable, so its proxy and CA keys are
# not installation transport.

WORKSPACE_REFUSALS: dict[str, dict[str, object]] = {
    "api-key-helper": {"settings.json": {"apiKeyHelper": "/workspace/key.sh"}},
    "local-env-base-url": {
        "settings.local.json": {"env": {"ANTHROPIC_BASE_URL": f"https://{ALT_BASE_HOST}"}}
    },
    "env-bedrock-selector": {"settings.json": {"env": {"CLAUDE_CODE_USE_BEDROCK": "1"}}},
    "env-https-proxy": {"settings.json": {"env": {"HTTPS_PROXY": "http://127.0.0.1:9"}}},
    "local-env-extra-ca": {
        "settings.local.json": {"env": {"NODE_EXTRA_CA_CERTS": "/workspace/ca.pem"}}
    },
    "env-oauth-token": {"settings.json": {"env": {GRANT_ENV: SERVICE_OAUTH}}},
    "model-overrides": {
        "settings.json": {"modelOverrides": {"claude-opus-5-5": "arn:example:model"}}
    },
    "force-login-method": {"settings.json": {"forceLoginMethod": "console"}},
    "env-not-an-object": {"settings.json": {"env": "ANTHROPIC_API_KEY=x"}},
    "invalid-json": {"settings.json": "{not json"},
    "local-not-an-object": {"settings.local.json": "[]"},
}


@pytest.mark.parametrize("settings", list(WORKSPACE_REFUSALS))
def test_workspace_settings_with_a_competing_source_are_refused_before_the_cli_starts(
    install: Installation, monkeypatch: pytest.MonkeyPatch, settings: str
) -> None:
    personal(monkeypatch)
    plant_managed(install.workspace / ".claude", WORKSPACE_REFUSALS[settings])
    wiring = wire_session(install, reviewer_model=REVIEWER_MODEL)

    with pytest.raises(PersonalLaunchRefusedError) as refused:
        connect_session(wiring)

    assert no_placeholder_in(str(refused.value)) == []
    assert_cli_never_started(install.proxy)


@pytest.mark.skipif(os.geteuid() == 0, reason="root reads a mode 000 file")
def test_unreadable_workspace_settings_are_refused_before_the_cli_starts(
    install: Installation, monkeypatch: pytest.MonkeyPatch
) -> None:
    personal(monkeypatch)
    settings = write_json(install.workspace / ".claude" / "settings.json", {"permissions": {}})
    settings.chmod(0o000)
    try:
        wiring = wire_session(install, reviewer_model=REVIEWER_MODEL)
        with pytest.raises(PersonalLaunchRefusedError):
            connect_session(wiring)
    finally:
        settings.chmod(0o600)

    assert_cli_never_started(install.proxy)


@requires_cli
def test_workspace_settings_without_a_competing_source_still_launch(
    install: Installation, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Liveness for every workspace refusal above.
    personal(monkeypatch)
    plant_managed(
        install.workspace / ".claude",
        {
            "settings.json": {"permissions": {"allow": ["Read"]}, "env": {"FOO": "bar"}},
            "settings.local.json": {"permissions": {"deny": []}},
        },
    )

    outcome = run_turn(wire_session(install, reviewer_model=REVIEWER_MODEL))

    assert outcome.final.status is SessionStatus.DONE, outcome.errors
    assert_owner_grant_only(install.proxy)


# --- AC 2: no owner grant means authentication required, before the CLI ----------------------


@pytest.mark.parametrize("grant", [None, "", "   \t"], ids=["absent", "empty", "blank"])
def test_a_missing_grant_is_refused_before_the_cli_with_every_competitor_present(
    install: Installation, monkeypatch: pytest.MonkeyPatch, grant: str | None
) -> None:
    plant_everything(install, monkeypatch)
    monkeypatch.setenv("CURIE_CREDENTIALS", SERVICE_OAUTH)
    personal(monkeypatch, grant=grant)
    wiring = wire_session(install, reviewer_model=REVIEWER_MODEL)

    with pytest.raises(AuthenticationRequiredError) as refused:
        connect_session(wiring)

    assert "authentication required" in str(refused.value)
    assert no_placeholder_in(str(refused.value)) == []
    assert_cli_never_started(install.proxy)


def test_an_explicit_empty_grant_fences_the_inherited_one(
    install: Installation, monkeypatch: pytest.MonkeyPatch
) -> None:
    personal(monkeypatch)
    wiring = wire_session(install, reviewer_model=REVIEWER_MODEL, explicit_env={GRANT_ENV: ""})

    with pytest.raises(AuthenticationRequiredError):
        connect_session(wiring)

    assert_cli_never_started(install.proxy)


def test_a_missing_grant_surfaces_as_an_authentication_required_turn(
    install: Installation, monkeypatch: pytest.MonkeyPatch
) -> None:
    plant_everything(install, monkeypatch)
    personal(monkeypatch, grant=None)

    outcome: TurnOutcome = run_turn(wire_session(install, reviewer_model=REVIEWER_MODEL))

    assert outcome.final.status is SessionStatus.CLASSIFIED_FAILURE
    [error] = outcome.errors
    assert "authentication required" in error.message
    assert no_placeholder_in(error.message) == []
    assert_cli_never_started(install.proxy)


# --- AC 2: unknown modes refuse; service mode needs no selector ------------------------------

UNKNOWN_MODES = ["Personal", "personal ", "", "oauth", "PERSONAL", " service"]


@pytest.mark.parametrize("mode", UNKNOWN_MODES)
def test_an_unknown_mode_is_refused_at_boot_build_and_connect(
    install: Installation, monkeypatch: pytest.MonkeyPatch, mode: str
) -> None:
    boot_env = {AUTH_MODE_ENV: mode, "CURIE_CREDENTIALS": SERVICE_OAUTH}
    with pytest.raises(InvalidAuthModeError):
        resolve_sdk_env(boot_env)
    assert GRANT_ENV not in boot_env, "an invalid mode still mapped the service credential"

    with pytest.raises(InvalidAuthModeError):
        build_options(
            plugins=[],
            model=None,
            system_prompt=None,
            max_turns=1,
            max_budget_usd=None,
            resume=None,
            reviewer_model=REVIEWER_MODEL,
            env={AUTH_MODE_ENV: mode},
        )

    monkeypatch.setenv(GRANT_ENV, OWNER_GRANT)
    wiring = wire_session(install)  # the mode is absent while the options are built
    monkeypatch.setenv(AUTH_MODE_ENV, mode)
    with pytest.raises(InvalidAuthModeError):
        connect_session(wiring)
    assert_cli_never_started(install.proxy)


@pytest.mark.parametrize(
    ("inherited", "explicit"),
    [("personal", "service"), (None, "personal")],
    ids=["explicit-service-cannot-downgrade", "explicit-personal-applies"],
)
def test_personal_wins_when_either_env_says_personal(
    install: Installation,
    monkeypatch: pytest.MonkeyPatch,
    inherited: str | None,
    explicit: str,
) -> None:
    if inherited is not None:
        monkeypatch.setenv(AUTH_MODE_ENV, inherited)
        # A personal boot env never maps the service credential, so it is no grant.
        monkeypatch.setenv("CURIE_CREDENTIALS", SERVICE_OAUTH)

    with pytest.raises(ReviewerModelRequiredError):
        wire_session(install, explicit_env={AUTH_MODE_ENV: explicit})
    wiring = wire_session(
        install, reviewer_model=REVIEWER_MODEL, explicit_env={AUTH_MODE_ENV: explicit}
    )
    with pytest.raises(AuthenticationRequiredError):
        connect_session(wiring)
    assert_cli_never_started(install.proxy)


# --- AC 2: personal mode needs an explicit Claude reviewer model ----------------------------


@pytest.mark.parametrize(
    "reviewer",
    [None, "", "openai/gpt-6.1-sol", "opus", "anthropic/claude-opus-5-5"],
    ids=["absent", "empty", "openai", "alias", "provider-prefixed"],
)
def test_personal_mode_refuses_a_missing_or_non_claude_reviewer_model(
    install: Installation, monkeypatch: pytest.MonkeyPatch, reviewer: str | None
) -> None:
    personal(monkeypatch)
    # An ambient default is not a configured reviewer: it must not satisfy this.
    monkeypatch.setenv(REVIEWER_ENV, REVIEWER_MODEL)

    with pytest.raises(ReviewerModelRequiredError) as refused:
        wire_session(install, reviewer_model=reviewer)

    assert no_placeholder_in(str(refused.value)) == []
    assert_cli_never_started(install.proxy)


# --- service mode keeps today's behavior ---------------------------------------------------


@requires_cli
@pytest.mark.parametrize("mode", [None, "service"], ids=["mode-absent", "mode-service"])
def test_service_mode_authenticates_with_the_service_credential_as_today(
    install: Installation, monkeypatch: pytest.MonkeyPatch, mode: str | None
) -> None:
    # The positive control for every AC 2 refusal: no owner grant, no reviewer
    # override, and the platform credential reference is mapped as before.
    if mode is not None:
        monkeypatch.setenv(AUTH_MODE_ENV, mode)
    monkeypatch.setenv("CURIE_CREDENTIALS", SERVICE_OAUTH)

    wiring = wire_session(install)
    outcome = run_turn(wiring)

    assert wiring.options.env[REVIEWER_ENV] == "claude-opus-5-5"
    assert outcome.final.status is SessionStatus.DONE, outcome.errors
    [request, *_] = install.proxy.model_requests()
    assert request.host == FIRST_PARTY_HOST
    assert request.bearer == "service oauth", request.bearer
    assert request.api_key is None, request.api_key


# --- the mode and classifier helpers -----------------------------------------------------


def test_the_mode_selector_reads_exactly_service_or_personal() -> None:
    assert AUTH_MODE_ENV == "CURIE_MODEL_AUTH_MODE"
    assert resolve_auth_mode({}) is AuthMode.SERVICE
    assert resolve_auth_mode({AUTH_MODE_ENV: "service"}) is AuthMode.SERVICE
    assert resolve_auth_mode({AUTH_MODE_ENV: "personal"}) is AuthMode.PERSONAL
    for mode in UNKNOWN_MODES:
        with pytest.raises(InvalidAuthModeError):
            resolve_auth_mode({AUTH_MODE_ENV: mode})
    for error in (
        InvalidAuthModeError,
        AuthenticationRequiredError,
        ReviewerModelRequiredError,
        PersonalLaunchRefusedError,
    ):
        assert issubclass(error, PersonalAuthError)
    assert issubclass(PersonalAuthError, RuntimeError)


COMPETING = [
    "ANTHROPIC_API_KEY",
    "ANTHROPIC_AUTH_TOKEN",
    "ANTHROPIC_FOUNDRY_API_KEY",
    "ANTHROPIC_FOUNDRY_RESOURCE",
    "ANTHROPIC_CUSTOM_HEADERS",
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_BEDROCK_BASE_URL",
    "ANTHROPIC_VERTEX_PROJECT_ID",
    "ANTHROPIC_PROFILE",
    "ANTHROPIC_FEDERATION_RULE_ID",
    "ANTHROPIC_CONFIG_DIR",
    "CURIE_MODEL_BASE_URL",
    "CURIE_CREDENTIALS",
    "CLAUDE_CONFIG_DIR",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "CLAUDE_CODE_USE_BEDROCK",
    "CLAUDE_CODE_USE_VERTEX",
    "CLAUDE_CODE_USE_FOUNDRY",
    "CLAUDE_CODE_USE_GATEWAY",
    "CLAUDE_CODE_USE_MANTLE",
    "CLAUDE_CODE_USE_ANTHROPIC_AWS",
    "CLAUDE_CODE_GATEWAY_TOKEN",
    "CLAUDE_CODE_GATEWAY_TOKEN_FILE_DESCRIPTOR",
    "CLAUDE_CODE_API_BASE_URL",
    "CLAUDE_CODE_SESSION_ACCESS_TOKEN",
    "CLAUDE_CODE_SKIP_BEDROCK_AUTH",
    "CLAUDE_CODE_CLIENT_CERT",
    "CLAUDE_CODE_CLIENT_KEY_PASSPHRASE",
    "CLAUDE_CODE_PROVIDER_MANAGED_BY_HOST",
    "CLAUDE_LOCAL_OAUTH_API_BASE",
    "CLOUD_ML_REGION",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "GOOGLE_CLOUD_PROJECT",
    "GCLOUD_PROJECT",
    "AWS_REGION",
    "AWS_BEARER_TOKEN_BEDROCK",
    "AZURE_CLIENT_ID",
    "VERTEX_REGION_CLAUDE_SONNET_5_5",
]
KEPT = [
    "ANTHROPIC_MODEL",
    "ANTHROPIC_SMALL_FAST_MODEL",
    "ANTHROPIC_DEFAULT_OPUS_MODEL",
    "ANTHROPIC_DEFAULT_HAIKU_MODEL",
    "ANTHROPIC_DEFAULT_SONNET_MODEL_NAME",
    "ANTHROPIC_DEFAULT_OPUS_MODEL_DESCRIPTION",
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "NO_PROXY",
    "NODE_EXTRA_CA_CERTS",
    "CLAUDE_CODE_SHELL",
    "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC",
    "CLAUDE_CODE_MAX_OUTPUT_TOKENS",
    "PATH",
    "HOME",
]


def test_the_classifier_strips_provider_namespaces_and_keeps_model_selection() -> None:
    # Provider selectors, credentials and endpoints named in the CLI docs
    # (https://code.claude.com/docs/en/env-vars, .../authentication) and the
    # WIF reference, plus selectors found in the pinned CLI 2.1.301 binary.
    assert [name for name in COMPETING if not is_competing_auth_key(name)] == []
    assert [name for name in KEPT if is_competing_auth_key(name)] == []
