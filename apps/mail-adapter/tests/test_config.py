"""MailAdapterConfig: the documented env surface, and nothing else.

Modeled on `apps/dispatcher/tests/test_config.py`. The promoted spike read
`os.environ` at import time into module constants with bare generic names
(`PORT`, `POLL_INTERVAL`), which is both untestable and a collision hazard in a
pod env. These tests pin the replacement: one frozen `BaseSettings` whose
aliased fields read ONLY their alias, so a stray bare-name variable cannot leak
in, and whose documented table cannot drift from the code.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
from curie_mail_adapter.config import MailAdapterConfig
from pydantic import AliasChoices

README = Path(__file__).resolve().parent.parent / "README.md"

# Every env var the adapter reads, paired with a distinct sentinel and the value
# it must coerce to. This is the table the chart template and the README are
# pinned against from the other side (chart render assertion 9), so a name typo
# in any one of the three cannot pass all three.
_OVERRIDES: dict[str, tuple[str, str, object]] = {
    # env var -> (field name, raw env value, expected coerced value)
    "AGENTMAIL_API_KEY": ("agentmail_api_key", "am-sentinel-key", "am-sentinel-key"),
    "AGENTMAIL_INBOX": ("agentmail_inbox", "sentinel@agentmail.to", "sentinel@agentmail.to"),
    "AGENTMAIL_BASE_URL": (
        "agentmail_base_url",
        "https://sentinel.example/v9",
        "https://sentinel.example/v9",
    ),
    "CURIE_API_URL": ("api_base_url", "http://sentinel-api:9000", "http://sentinel-api:9000"),
    "CURIE_CHANNEL_TOKEN": ("channel_token", "chn-sentinel", "chn-sentinel"),
    "CURIE_EGRESS_SECRET": ("egress_secret", "egress-sentinel", "egress-sentinel"),
    "ADAPTER_INGRESS_ENABLED": ("ingress_enabled", "false", False),
    "CURIE_MAIL_POLL_INTERVAL_SECONDS": ("poll_interval_seconds", "12.5", 12.5),
    "CURIE_MAIL_INGRESS_ATTEMPTS": ("ingress_attempts", "7", 7),
    "CURIE_MAIL_INGRESS_RETRY_DELAY_SECONDS": ("ingress_retry_delay_seconds", "1.5", 1.5),
    "CURIE_MAIL_PORT": ("port", "8123", 8123),
    "CURIE_MAIL_ALLOWED_SENDERS": (
        "allowed_senders",
        "alice@example.com,example.org",
        ("alice@example.com", "example.org"),
    ),
}

_DEFAULTS: dict[str, object] = {
    "agentmail_api_key": "",
    "agentmail_inbox": "",
    "agentmail_base_url": "https://api.agentmail.to/v0",
    "api_base_url": "http://localhost:8000",
    "channel_token": "",
    "egress_secret": "",
    "ingress_enabled": True,
    "poll_interval_seconds": 5.0,
    "ingress_attempts": 3,
    "ingress_retry_delay_seconds": 2.0,
    "port": 8080,
    "allowed_senders": (),
}


def _env_names(field_name: str, field: object) -> list[str]:
    """Every env var name this field can be read from."""
    alias = getattr(field, "validation_alias", None)
    if isinstance(alias, str):
        return [alias]
    if isinstance(alias, AliasChoices):
        return [choice for choice in alias.choices if isinstance(choice, str)]
    return [field_name.upper()]


def _clear_all_config_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Delete every env var the config could read, for a clean-env baseline."""
    for name, field in MailAdapterConfig.model_fields.items():
        for env_name in _env_names(name, field):
            monkeypatch.delenv(env_name, raising=False)


def test_defaults_on_a_clean_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every documented default, enumerated. A drifted default is a silent prod break."""
    _clear_all_config_env(monkeypatch)

    config = MailAdapterConfig()

    for field, expected in _DEFAULTS.items():
        actual = getattr(config, field)
        assert actual == expected, f"{field}: {actual!r} != {expected!r}"
        assert type(actual) is type(expected), f"{field}: {type(actual)} != {type(expected)}"
    assert set(_DEFAULTS) == set(MailAdapterConfig.model_fields), (
        "a field was added or removed without updating this table (and the README)"
    )


def test_every_env_var_overrides_its_field(monkeypatch: pytest.MonkeyPatch) -> None:
    """Each documented name, set under its EXACT spelling, reaches the right field.

    This is the test that catches a name typo between the config, the chart
    template and the README.
    """
    _clear_all_config_env(monkeypatch)
    for env_var, (_field, raw, _expected) in _OVERRIDES.items():
        monkeypatch.setenv(env_var, raw)

    config = MailAdapterConfig()

    for env_var, (field, _raw, expected) in _OVERRIDES.items():
        actual = getattr(config, field)
        assert actual == expected, f"{env_var} -> {field}: {actual!r} != {expected!r}"
        assert type(actual) is type(expected), (
            f"{env_var} -> {field}: type {type(actual)} != {type(expected)}"
        )


def test_a_bare_uppercased_field_name_is_not_read(monkeypatch: pytest.MonkeyPatch) -> None:
    """A stray generic pod-env variable must not leak into an aliased field.

    `PORT` and `POLL_INTERVAL` are the exact collision hazard that renaming the
    spike's env surface to `CURIE_MAIL_*` exists to remove; this pins the
    `AliasOnlyEnvSource` wiring that makes the rename real rather than cosmetic.
    """
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("PORT", "9999")
    monkeypatch.setenv("POLL_INTERVAL", "99")
    monkeypatch.setenv("POLL_INTERVAL_SECONDS", "99")
    monkeypatch.setenv("ALLOWED_SENDERS", "*")
    monkeypatch.setenv("INGRESS_ENABLED", "false")

    config = MailAdapterConfig()

    assert config.port == 8080
    assert config.poll_interval_seconds == 5.0
    assert config.allowed_senders == ()
    assert config.ingress_enabled is True


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("a@b.com,,c@d.com", ("a@b.com", "c@d.com")),
        ("a@b.com,c@d.com,", ("a@b.com", "c@d.com")),
        ("  a@b.com , c@d.com  ", ("a@b.com", "c@d.com")),
        ("", ()),
        (",", ()),
        ("*", ("*",)),
    ],
)
def test_allowed_senders_parses_a_comma_separated_string(
    monkeypatch: pytest.MonkeyPatch, raw: str, expected: tuple[str, ...]
) -> None:
    """Empty segments are dropped, never treated as a match-anything entry.

    A trailing comma is the common operator typo, and a bug here converts a
    configured allow-list into an open one.
    """
    _clear_all_config_env(monkeypatch)
    monkeypatch.setenv("CURIE_MAIL_ALLOWED_SENDERS", raw)

    assert MailAdapterConfig().allowed_senders == expected


def test_the_readme_config_table_matches_the_code() -> None:
    """The published config table and `MailAdapterConfig` cannot drift apart.

    The README table is the operator's contract; a documented variable nothing
    reads, or a read variable nobody documented, are both defects the docs gate
    cannot see.
    """
    documented = set(re.findall(r"^\|\s*`([A-Z][A-Z0-9_]*)`\s*\|", README.read_text(), re.M))
    assert documented, f"no config table rows found in {README}"

    readable: set[str] = set()
    for name, field in MailAdapterConfig.model_fields.items():
        names = _env_names(name, field)
        readable.update(names)
        assert documented.intersection(names), f"{name} is read from {names} but undocumented"

    assert documented <= readable, f"documented but never read: {sorted(documented - readable)}"
