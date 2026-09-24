"""The connector's configuration, read once from the environment."""

import json
from collections.abc import Mapping
from dataclasses import dataclass, field

MAX_PROBES_CEILING = 4  # ADR 0169 d4
MAX_CONCURRENT_ROUNDS_CEILING = 4  # ADR 0169 d7: concurrent rounds per address
CREDENTIALS_VAR = "MEAN_TESTER_CREDENTIALS"


class ConfigError(ValueError):
    """The connector cannot start with this environment."""


@dataclass(frozen=True)
class RepoRef:
    owner: str
    name: str
    ref: str

    @property
    def full_name(self) -> str:
        return f"{self.owner}/{self.name}"


@dataclass(frozen=True)
class Config:
    slack_token: str
    github_token: str
    channels: frozenset[str]
    repos: tuple[RepoRef, ...]
    max_probes: int = MAX_PROBES_CEILING
    reply_timeout_s: float = 240.0
    settle_s: float = 20.0
    max_probe_chars: int = 1500
    max_concurrent_rounds: int = 2
    # bundle name -> repository-relative directory of its specification (d2)
    spec_paths: dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_env(cls, env: Mapping[str, str]) -> "Config":
        required = (CREDENTIALS_VAR, "MEAN_TESTER_CHANNELS", "MEAN_TESTER_REPOS")
        missing = [name for name in required if not env.get(name, "").strip()]
        if missing:
            raise ConfigError("missing " + ", ".join(missing))
        slack_token, github_token = _credentials(env[CREDENTIALS_VAR])
        max_probes = _bounded(env, "MEAN_TESTER_MAX_PROBES", MAX_PROBES_CEILING, MAX_PROBES_CEILING)
        max_concurrent_rounds = _bounded(
            env, "MEAN_TESTER_MAX_CONCURRENT_ROUNDS", 2, MAX_CONCURRENT_ROUNDS_CEILING
        )
        return cls(
            slack_token=slack_token,
            github_token=github_token,
            channels=frozenset(_split(env["MEAN_TESTER_CHANNELS"])),
            repos=tuple(_repo(item) for item in _split(env["MEAN_TESTER_REPOS"])),
            max_probes=max_probes,
            max_concurrent_rounds=max_concurrent_rounds,
            reply_timeout_s=float(env.get("MEAN_TESTER_REPLY_TIMEOUT_S", "240")),
            settle_s=float(env.get("MEAN_TESTER_SETTLE_S", "20")),
            spec_paths=_spec_paths(env.get("MEAN_TESTER_SPEC_PATHS", "")),
        )


def _bounded(env: Mapping[str, str], name: str, default: int, ceiling: int) -> int:
    """An integer the operator can lower but never raise past `ceiling`."""
    value = int(env.get(name, str(default)))
    if not 1 <= value <= ceiling:
        raise ConfigError(f"{name} must be 1..{ceiling}, got {value}")
    return value


def _credentials(raw: str) -> tuple[str, str]:
    """Parse `MEAN_TESTER_CREDENTIALS`: a JSON object with `slack_bot_token` and
    `github_token`. One SecretRef rather than two, because two are refused
    upstream without a `bearer_secret` (`plugin_format/connectors.py`), and a
    `bearer_secret` would derive a header the sandbox can never expand -- the
    runner only drops that unreachable header for a LONE SecretRef
    (`runner/src/curie_runner/connectors.py`).

    Every error names `MEAN_TESTER_CREDENTIALS` and, when it applies, the
    missing key -- never a token value. The blob itself is a credential, and an
    exception message is the one place a token routinely leaks into a log.
    """

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{CREDENTIALS_VAR} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ConfigError(f"{CREDENTIALS_VAR} must be a JSON object")
    missing = [
        key for key in ("slack_bot_token", "github_token") if not str(parsed.get(key, "")).strip()
    ]
    if missing:
        raise ConfigError(f"{CREDENTIALS_VAR} is missing " + ", ".join(missing))
    return str(parsed["slack_bot_token"]).strip(), str(parsed["github_token"]).strip()


def _spec_paths(raw: str) -> dict[str, str]:
    """Parse the optional `MEAN_TESTER_SPEC_PATHS`: a JSON object mapping a
    bundle name to a repository-relative directory."""
    if not raw.strip():
        return {}
    name = "MEAN_TESTER_SPEC_PATHS"
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ConfigError(f"{name} is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ConfigError(f"{name} must be a JSON object of bundle name to directory")
    out = {}
    for bundle, directory in parsed.items():
        if not isinstance(directory, str):
            raise ConfigError(f"{name}[{bundle!r}] must be a string")
        clean = directory.strip().strip("/")
        if not clean or directory.startswith("/") or ".." in clean.split("/"):
            raise ConfigError(f"{name}[{bundle!r}] must be a repository-relative directory")
        out[str(bundle)] = clean
    return out


def _split(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _repo(item: str) -> RepoRef:
    slug, sep, ref = item.partition("@")
    owner, slash, name = slug.partition("/")
    if not (sep and slash and owner and name and ref):
        raise ConfigError(f"MEAN_TESTER_REPOS entry {item!r} is not owner/name@ref")
    return RepoRef(owner, name, ref)
