"""Personal model authentication for the Claude harness (#5373, ADR 0268 section 1).

``CURIE_MODEL_AUTH_MODE`` selects who pays for model work. Absent or
``service`` keeps every existing service path. ``personal`` makes the owner's
``CLAUDE_CODE_OAUTH_TOKEN`` the only model grant: no service credential, no
saved login, profile or helper, and no alternate endpoint may stand in for it
or outrank it. Any other present value is refused, so a misrendered personal
install never falls back to service billing.

The native CLI's credential precedence is documented at
https://code.claude.com/docs/en/authentication ("Authentication precedence").
Cloud selectors, ``ANTHROPIC_AUTH_TOKEN``, ``ANTHROPIC_API_KEY`` and
``apiKeyHelper`` all outrank ``CLAUDE_CODE_OAUTH_TOKEN``, and a signed-in
gateway session or managed policy that forces gateway login outranks the whole
list. This module therefore removes every removable source from the CLI parent
env, points the CLI at fresh config directories, and refuses to launch when a
workspace or managed settings file would reintroduce a competing source.

Error messages name variables, settings keys and files. They never carry a
credential value.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from collections.abc import Callable, Collection, Mapping, MutableMapping
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path

from aci_protocol import BootEnv

from .sdk_auth import (
    API_KEY_ENV,
    AUTH_TOKEN_ENV,
    BASE_URL_ENV,
    CREDENTIALS_ENV,
    MODEL_BASE_URL_ENV,
    OAUTH_TOKEN_ENV,
)

AUTH_MODE_ENV = "CURIE_MODEL_AUTH_MODE"
REVIEWER_MODEL_ENV = BootEnv.env_key("reviewer_model")
CLAUDE_CONFIG_DIR_ENV = "CLAUDE_CONFIG_DIR"
ANTHROPIC_CONFIG_DIR_ENV = "ANTHROPIC_CONFIG_DIR"

# Linux managed settings location. The pinned CLI resolves it with no override
# (https://code.claude.com/docs/en/managed-settings). Read at call time.
MANAGED_SETTINGS_DIR = Path("/etc/claude-code")
_MANAGED_FILE = "managed-settings.json"
_MANAGED_DROP_IN_DIR = "managed-settings.d"


class AuthMode(StrEnum):
    """Who authenticates model work. The values are the ``CURIE_MODEL_AUTH_MODE`` wire."""

    SERVICE = "service"
    PERSONAL = "personal"


class PersonalAuthError(RuntimeError):
    """A personal-mode policy refusal raised before the CLI starts."""


class InvalidAuthModeError(PersonalAuthError):
    """``CURIE_MODEL_AUTH_MODE`` is present but not exactly ``service`` or ``personal``."""


class AuthenticationRequiredError(PersonalAuthError):
    """Personal mode has no owner grant."""


class ReviewerModelRequiredError(PersonalAuthError):
    """Personal mode has no explicit Claude reviewer model."""


class PersonalLaunchRefusedError(PersonalAuthError):
    """Workspace or managed settings would add a competing credential source."""


def resolve_auth_mode(env: Mapping[str, str]) -> AuthMode:
    """The mode one env selects. Absent is service; no case folding or stripping."""

    if AUTH_MODE_ENV not in env:
        return AuthMode.SERVICE
    try:
        return AuthMode(env[AUTH_MODE_ENV])
    except ValueError:
        raise InvalidAuthModeError(
            f"{AUTH_MODE_ENV} must be exactly {AuthMode.SERVICE.value!r} or "
            f"{AuthMode.PERSONAL.value!r}, or unset for service."
        ) from None


def effective_auth_mode(inherited: Mapping[str, str], explicit: Mapping[str, str]) -> AuthMode:
    """Merge the inherited and explicit SDK env selections, failing closed.

    Both present values are validated. Personal wins when either says personal,
    so an explicit ``service`` cannot downgrade a personal boot env.
    """

    modes = {resolve_auth_mode(inherited), resolve_auth_mode(explicit)}
    return AuthMode.PERSONAL if AuthMode.PERSONAL in modes else AuthMode.SERVICE


# --- competing credential sources ------------------------------------------------
#
# Selectors, credentials and endpoints from https://code.claude.com/docs/en/env-vars
# and .../authentication, plus names found in the pinned CLI 2.1.301 binary.

_COMPETING_NAMES = frozenset(
    {
        API_KEY_ENV,
        AUTH_TOKEN_ENV,
        OAUTH_TOKEN_ENV,  # re-set to the owner grant by personal_cli_env
        "ANTHROPIC_FOUNDRY_API_KEY",
        "ANTHROPIC_CUSTOM_HEADERS",
        BASE_URL_ENV,
        MODEL_BASE_URL_ENV,
        CREDENTIALS_ENV,
        CLAUDE_CONFIG_DIR_ENV,
        ANTHROPIC_CONFIG_DIR_ENV,
        "CLOUD_ML_REGION",
        "GOOGLE_APPLICATION_CREDENTIALS",
        "GOOGLE_CLOUD_PROJECT",
        "GCLOUD_PROJECT",
    }
)
_COMPETING_PREFIXES = (
    "CLAUDE_CODE_USE_",
    "AWS_",
    "AZURE_",
    "VERTEX_REGION_",
    "CLAUDE_BG_",
    "CLAUDE_LOCAL_OAUTH_",
)
# Model selection is the one ANTHROPIC_* family personal mode keeps.
_MODEL_SELECTION = re.compile(
    r"ANTHROPIC_(?:MODEL|SMALL_FAST_MODEL|DEFAULT_[A-Z0-9]+_MODEL(?:_NAME|_DESCRIPTION)?)"
)
# A CLAUDE_* name carrying one of these words is treated as auth or routing,
# which covers undocumented gateway, descriptor, mTLS and skip-auth variables.
_CLAUDE_AUTH_WORDS = frozenset(
    {
        "TOKEN",
        "OAUTH",
        "AUTH",
        "KEY",
        "CREDENTIAL",
        "CREDENTIALS",
        "CERT",
        "PASSPHRASE",
        "GATEWAY",
        "DESCRIPTOR",
        "PROVIDER",
    }
)


def is_competing_auth_key(name: str) -> bool:
    """Whether ``name`` selects, carries or routes a model credential other than the grant.

    Fails closed on the provider namespaces. Installation transport
    (``HTTPS_PROXY``, ``HTTP_PROXY``, ``NO_PROXY``, ``NODE_EXTRA_CA_CERTS``) and
    model selection are kept. ``CLAUDE_CODE_OAUTH_TOKEN`` counts as competing so
    no inherited copy survives; the policy sets the owner grant itself.
    """

    if name in _COMPETING_NAMES or name.startswith(_COMPETING_PREFIXES):
        return True
    if name.startswith("ANTHROPIC_"):
        return _MODEL_SELECTION.fullmatch(name) is None
    if name.startswith("CLAUDE_"):
        return "BASE_URL" in name or not _CLAUDE_AUTH_WORDS.isdisjoint(name.split("_"))
    return False


# --- reviewer model and owner grant ----------------------------------------------

_CLAUDE_MODEL_ID = re.compile(r"claude-[a-z0-9][a-z0-9.-]*(?:\[1m\])?")


def require_claude_reviewer_model(reviewer_model: str | None) -> str:
    """The explicit Claude reviewer model personal mode requires.

    No credential-prefix default and no ambient ``ANTHROPIC_DEFAULT_OPUS_MODEL``
    stands in for it. Aliases and provider-prefixed IDs are not Claude model IDs.
    """

    if reviewer_model is None or _CLAUDE_MODEL_ID.fullmatch(reviewer_model) is None:
        raise ReviewerModelRequiredError(
            f"personal model authentication needs an explicit Claude reviewer model in "
            f"{REVIEWER_MODEL_ENV} (a claude-* model ID); no default applies."
        )
    return reviewer_model


def require_owner_grant(env: Mapping[str, str]) -> str:
    """The owner's grant from the combined env; absent, empty or blank is refused."""

    grant = env.get(OAUTH_TOKEN_ENV, "")
    if not grant.strip():
        raise AuthenticationRequiredError(
            f"authentication required: personal model authentication uses only the "
            f"owner's {OAUTH_TOKEN_ENV}, which is absent or empty. No service "
            f"credential or saved login is used in its place."
        )
    return grant


# --- settings inspection -----------------------------------------------------------
#
# Settings keys from https://code.claude.com/docs/en/settings and
# .../authentication. Each refusal names the file and the keys, never a value.

_CREDENTIAL_HELPER_KEYS = frozenset(
    {"apiKeyHelper", "awsAuthRefresh", "awsCredentialExport", "gcpAuthRefresh"}
)
_WORKSPACE_REFUSED_KEYS = _CREDENTIAL_HELPER_KEYS | {
    "forceLoginMethod",
    "forceLoginGatewayUrl",
    "forceLoginOrgUUID",
    "gatewayInternalNetworks",
    "modelOverrides",
}
# policyHelper computes policy at launch, so it cannot be classified here.
_MANAGED_REFUSED_KEYS = _CREDENTIAL_HELPER_KEYS | {"forceLoginGatewayUrl", "policyHelper"}
# Workspace settings are agent writable, so their transport keys are not
# installation-controlled. Compared case-insensitively.
_WORKSPACE_TRANSPORT_KEYS = frozenset(
    {
        "HTTPS_PROXY",
        "HTTP_PROXY",
        "ALL_PROXY",
        "NO_PROXY",
        "NODE_EXTRA_CA_CERTS",
        "SSL_CERT_FILE",
        "SSL_CERT_DIR",
    }
)


def _refused(source: str, path: Path, reason: str) -> PersonalLaunchRefusedError:
    return PersonalLaunchRefusedError(f"personal launch refused: {source} {path} {reason}.")


def _read_settings(source: str, path: Path) -> dict[str, object] | None:
    """A settings file as a JSON object, or None when it does not exist.

    Any other read failure, invalid JSON or a non-object is a refusal: a file
    the CLI would load but this guard cannot read cannot be classified.
    """

    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise _refused(source, path, f"cannot be read ({type(exc).__name__})") from None
    try:
        parsed: object = json.loads(raw.decode("utf-8-sig"))
    except ValueError:  # UnicodeDecodeError and JSONDecodeError
        raise _refused(source, path, "is not valid JSON") from None
    if not isinstance(parsed, dict):
        raise _refused(source, path, "is not a JSON object")
    return parsed


def _refused_policy_env(name: str) -> bool:
    return is_competing_auth_key(name) or name.startswith("ANTHROPIC_")


def _refused_workspace_env(name: str) -> bool:
    return _refused_policy_env(name) or name.upper() in _WORKSPACE_TRANSPORT_KEYS


def _settings_refusals(
    settings: Mapping[str, object],
    refused_keys: Collection[str],
    refused_env: Callable[[str], bool],
) -> list[str]:
    """The keys in one settings object that add or force a competing source."""

    found = sorted(key for key in settings if key in refused_keys)
    if "env" in settings:
        env = settings["env"]
        if not isinstance(env, dict):
            found.append("env (not an object)")
        else:
            found.extend(f"env.{name}" for name in sorted(env) if refused_env(name))
    return found


def inspect_workspace_settings(cwd: str | os.PathLike[str]) -> None:
    """Refuse when project or local settings in ``cwd`` add a competing source.

    These are the ``project`` and ``local`` setting sources ADR 0189 keeps
    loading: ``<cwd>/.claude/settings.json`` and ``settings.local.json``.
    """

    for name in ("settings.json", "settings.local.json"):
        path = Path(cwd) / ".claude" / name
        settings = _read_settings("workspace settings", path)
        if settings is None:
            continue
        found = _settings_refusals(settings, _WORKSPACE_REFUSED_KEYS, _refused_workspace_env)
        if found:
            raise _refused(
                "workspace settings",
                path,
                f"sets {', '.join(found)}, which would compete with the owner grant",
            )


def _managed_files(root: Path) -> list[Path]:
    """``managed-settings.json`` then non-hidden ``managed-settings.d/*.json``, sorted.

    Hidden files and files not ending in ``.json`` are ignored, as the CLI
    ignores them (https://code.claude.com/docs/en/managed-settings).
    """

    drop_ins = root / _MANAGED_DROP_IN_DIR
    try:
        names = sorted(os.listdir(drop_ins))
    except FileNotFoundError:
        names = []
    except OSError as exc:
        raise _refused(
            "managed settings", drop_ins, f"cannot be listed ({type(exc).__name__})"
        ) from None
    return [
        root / _MANAGED_FILE,
        *(drop_ins / name for name in names if name.endswith(".json") and not name.startswith(".")),
    ]


def inspect_managed_policy() -> None:
    """Refuse when mandatory managed policy forces or adds a competing source.

    The runner never edits, hides or bypasses managed policy. Policy that forces
    gateway login, names a credential helper or ``policyHelper``, or puts a
    competing variable in ``env`` is refused, as is a present file that cannot
    be read or parsed. Managed proxy and CA variables are installation
    transport and are allowed.
    """

    for path in _managed_files(MANAGED_SETTINGS_DIR):
        settings = _read_settings("managed settings", path)
        if settings is None:
            continue
        found = _settings_refusals(settings, _MANAGED_REFUSED_KEYS, _refused_policy_env)
        if settings.get("forceLoginMethod", "claudeai") != "claudeai":
            # gateway forces the gateway, console competes, others are unknown.
            found.append("forceLoginMethod")
        if found:
            raise _refused(
                "managed settings",
                path,
                f"sets {', '.join(found)}, which force or add a credential source other "
                f"than the owner grant; managed policy is not bypassed",
            )


# --- the CLI parent env --------------------------------------------------------------


@dataclass(frozen=True)
class PersonalConfigDirs:
    """Fresh runner-owned CLI config directories for one personal connect.

    ``CLAUDE_CONFIG_DIR`` holds the saved login, ``.claude.json`` and user
    settings, and ``ANTHROPIC_CONFIG_DIR`` holds named profiles
    (https://code.claude.com/docs/en/env-vars). Fresh empty directories keep
    every inherited saved source out, including what the SDK copies from the
    effective config dir when it materializes a resume.
    """

    claude: Path
    anthropic: Path

    @classmethod
    def create(cls) -> PersonalConfigDirs:
        claude = Path(tempfile.mkdtemp(prefix="curie-claude-config-"))
        anthropic = claude / "anthropic-config"
        try:
            anthropic.mkdir(mode=0o700)
        except BaseException:
            shutil.rmtree(claude)
            raise
        return cls(claude=claude, anthropic=anthropic)

    def env(self) -> dict[str, str]:
        return {
            CLAUDE_CONFIG_DIR_ENV: str(self.claude),
            ANTHROPIC_CONFIG_DIR_ENV: str(self.anthropic),
        }

    def remove(self) -> None:
        shutil.rmtree(self.claude)


def personal_cli_env(
    parent: Mapping[str, str],
    options_env: MutableMapping[str, str],
    *,
    grant: str,
    config: PersonalConfigDirs,
    removed: Collection[str] = (),
) -> dict[str, str]:
    """Build the CLI parent env for a personal launch.

    ``parent`` is the sanitized inherited env and ``options_env`` the explicit
    SDK env, which the SDK lays over the process env at spawn. ``removed`` adds
    names to drop, such as the platform credential names and the targets a
    ``CURIE_MODEL_ENV_KEY`` declaration resolves to.

    ``options_env`` is edited in place: competing names go and the fresh config
    dirs are set, so the SDK merge cannot restore a removed value. An explicit
    grant already there is left as it is (it is the grant); none is added.
    The returned env has every competing name removed, then the grant and the
    config dirs set.
    """

    def dropped(name: str) -> bool:
        return name in removed or is_competing_auth_key(name)

    for name in [name for name in options_env if dropped(name) and name != OAUTH_TOKEN_ENV]:
        del options_env[name]
    options_env.update(config.env())
    final = {name: value for name, value in {**parent, **options_env}.items() if not dropped(name)}
    final.update(config.env())
    final[OAUTH_TOKEN_ENV] = grant
    return final
