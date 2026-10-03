"""The backlog windows must stay strictly positive (#3720).

``backlog_reservation`` (``delivery.py``) floors ``time.time()`` by the window,
so a window of 0 divides by zero: every new hook delivery (``routers/hooks.py``)
and channel binding delivery (``routers/channels.py``) then fails with a 500
AFTER the delivery claim was taken, and the claim is not released on that path,
so retries see the delivery as pending until ``channel_delivery_lease_s``
expires. A negative window is no better: the quota script's ``EXPIRE`` goes
negative, which deletes the counter immediately, so the per-agent backlog quota
is silently never enforced. Both windows are therefore bounded at ``Settings``
construction, the same posture as ``GITHUB_FACTORY_RECONCILE_INTERVAL_S``
(#3709): a bad value is refused at boot rather than surfacing mid-delivery.

Pure unit tests: no fixtures, no Postgres/Valkey/network. ``_env_file=None``
only disables dotenv loading, so each case also clears the two ambient env
overrides -- a stray ``HOOK_BACKLOG_WINDOW_S`` in the test environment must
not mask the refusal under test.
"""

import pytest
from curie_api.config import Settings
from pydantic import ValidationError

_BACKLOG_WINDOW_FIELDS = (
    "hook_backlog_window_s",
    "channel_binding_backlog_window_s",
)
_BACKLOG_WINDOW_ENV = (
    "HOOK_BACKLOG_WINDOW_S",
    "CHANNEL_BINDING_BACKLOG_WINDOW_S",
)


def _clear_backlog_window_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in _BACKLOG_WINDOW_ENV:
        monkeypatch.delenv(name, raising=False)


@pytest.mark.parametrize("field", _BACKLOG_WINDOW_FIELDS)
@pytest.mark.parametrize("value", [0, -60])
def test_backlog_window_refuses_zero_or_negative(
    field: str, value: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The negative proof: 0 and -60 were accepted before the bound (#3720)."""

    _clear_backlog_window_env(monkeypatch)

    with pytest.raises(ValidationError):
        Settings(_env_file=None, **{field: value})


def test_backlog_windows_default_to_60(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_backlog_window_env(monkeypatch)

    settings = Settings(_env_file=None)

    assert settings.hook_backlog_window_s == 60
    assert settings.channel_binding_backlog_window_s == 60


@pytest.mark.parametrize("field", _BACKLOG_WINDOW_FIELDS)
def test_backlog_windows_accept_a_positive_override(
    field: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    _clear_backlog_window_env(monkeypatch)

    settings = Settings(_env_file=None, **{field: 3600})

    assert getattr(settings, field) == 3600
