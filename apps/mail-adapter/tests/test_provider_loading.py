"""How a mail provider is selected, loaded, versioned and bound to a state file.

ADR 0228 decision 4: ``agentmail`` resolves by direct import and never through
discovery; any other name comes from the ``curie.mail_provider`` entry-point
group with its guards; the interface version is checked at boot; and a state
file records its provider so ids from two mail services never mix. Sketch step
4 migrates an existing state file.
"""

from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from importlib.metadata import EntryPoint, EntryPoints
from pathlib import Path

import pytest
from _support import (
    IngressState,
    MailState,
    adapter_env,
    exit_of,
    free_port,
    spawn_adapter,
)
from _verifying_provider import VerifyingTestProvider
from curie_mail_adapter.adapter import MailAdapter
from curie_mail_adapter.agentmail import AgentMailProvider
from curie_mail_adapter.config import MailAdapterConfig
from curie_mail_adapter.provider import (
    INTERFACE_VERSION,
    ProviderLoadError,
    check_interface_version,
    provider_problems,
    resolve_factory,
)
from curie_mail_adapter.state import SCHEMA_VERSION
from curie_mail_adapter.state import MailState as DurableMailState

GROUP = "curie.mail_provider"


def _points(*entries: tuple[str, str]) -> Callable[[], EntryPoints]:
    return lambda: EntryPoints(EntryPoint(name, value, GROUP) for name, value in entries)


def _builtin() -> object:
    raise AssertionError("the built-in factory must not be called while resolving")


# --- selection ---------------------------------------------------------------------


def test_agentmail_resolves_to_the_builtin_without_discovery() -> None:
    def broken() -> EntryPoints:
        raise AssertionError("discovery ran for the built-in provider")

    assert resolve_factory("agentmail", _builtin, broken) is _builtin  # type: ignore[arg-type]


def test_another_name_resolves_through_its_one_entry_point() -> None:
    factory = resolve_factory(
        "example",
        _builtin,  # type: ignore[arg-type]
        _points(("example", "json.decoder:JSONDecoder")),
    )
    assert factory is json.decoder.JSONDecoder


@pytest.mark.parametrize(
    ("entries", "fragment"),
    [
        ((), "no installed package registers"),
        ((("example", "pkg_a.mod:make"), ("example", "pkg_b.mod:make")), "2 installed packages"),
        ((("example", "json.decoder:JSONDecoder"), ("agentmail", "pkg.mod:make")), "reserved"),
        ((("example", "flatmodule:make"),), "flat top-level module"),
        ((("example", "no_such_package_here.mod:make"),), "loading the provider failed"),
    ],
    ids=["unclaimed", "claimed_twice", "claims_agentmail", "flat_module", "import_fails"],
)
def test_the_loader_refuses_each_guarded_case_naming_the_variable(
    entries: tuple[tuple[str, str], ...], fragment: str
) -> None:
    with pytest.raises(ProviderLoadError) as caught:
        resolve_factory("example", _builtin, _points(*entries))  # type: ignore[arg-type]
    assert "CURIE_MAIL_PROVIDER" in str(caught.value)
    assert fragment in str(caught.value)


def test_an_unknown_provider_fails_boot_naming_the_variable(
    mail: MailState, ingress: IngressState
) -> None:
    env = adapter_env(
        agentmail_base_url=mail.base_url,
        api_url=ingress.url,
        port=free_port(),
        CURIE_MAIL_PROVIDER="no-such-provider",
    )

    code, output = exit_of(spawn_adapter(env), timeout=15)

    assert code != 0
    assert "CURIE_MAIL_PROVIDER" in output
    assert "no-such-provider" in output


# --- the interface version -----------------------------------------------------------


class _Versioned:
    def __init__(self, version: object) -> None:
        self.interface_version = version


@pytest.mark.parametrize(
    ("version", "boots"),
    [
        (INTERFACE_VERSION, True),
        ((INTERFACE_VERSION[0], max(0, INTERFACE_VERSION[1] - 1)), True),
        ((INTERFACE_VERSION[0], INTERFACE_VERSION[1] + 1), False),
        ((INTERFACE_VERSION[0] + 1, 0), False),
        ((INTERFACE_VERSION[0] - 1, INTERFACE_VERSION[1]), False),
        ("1.0", False),
    ],
    ids=["same", "older_minor", "newer_minor", "newer_major", "older_major", "not_a_version"],
)
def test_the_adapter_boots_the_same_major_with_an_equal_or_older_minor(
    version: object, boots: bool
) -> None:
    problem = check_interface_version(_Versioned(version))  # type: ignore[arg-type]
    assert (problem is None) is boots
    if isinstance(version, tuple) and not boots:
        assert f"{version[0]}.{version[1]}" in str(problem)
        assert f"{INTERFACE_VERSION[0]}.{INTERFACE_VERSION[1]}" in str(problem)


@pytest.mark.parametrize("inbox", ["", "Name <bot@example.com>", "a@example.com,b@example.com"])
def test_an_inbox_address_that_is_not_one_bare_address_fails_boot(
    make_config: Callable[..., MailAdapterConfig], inbox: str
) -> None:
    provider = AgentMailProvider(make_config(agentmail_inbox=inbox))
    assert any("inbox address" in problem for problem in provider_problems(provider))


def test_the_builtin_provider_passes_the_neutral_boot_checks(
    make_config: Callable[..., MailAdapterConfig],
) -> None:
    assert provider_problems(AgentMailProvider(make_config())) == []


# --- one state file, one provider ---------------------------------------------------


def test_a_state_file_refuses_a_different_provider(
    make_config: Callable[..., MailAdapterConfig],
) -> None:
    config = make_config()
    MailAdapter(config).close()

    with pytest.raises(RuntimeError, match="belongs to mail provider 'agentmail'"):
        MailAdapter(config, provider=VerifyingTestProvider(config))

    # The same provider still boots on it.
    MailAdapter(config).close()


def test_a_different_provider_on_a_bound_volume_fails_boot(
    mail: MailState, ingress: IngressState, tmp_path: Path
) -> None:
    state_path = tmp_path / "bound.sqlite3"
    store = DurableMailState(str(state_path), max_pending=10, max_bytes=1 << 24)
    store.bind_provider("another-provider")
    store.close()
    env = adapter_env(
        agentmail_base_url=mail.base_url,
        api_url=ingress.url,
        port=free_port(),
        CURIE_MAIL_STATE_PATH=str(state_path),
    )

    code, output = exit_of(spawn_adapter(env), timeout=15)

    assert code != 0
    assert "another-provider" in output
    assert "CURIE_MAIL_PROVIDER" in output


# --- the schema 4 migration -----------------------------------------------------------


def _schema_three_file(path: Path) -> None:
    """Build a state file as schema 3 left it, with pending rows and a raw listing."""
    store = DurableMailState(str(path), max_pending=10, max_bytes=1 << 24)
    listing = {"message_id": "msg-body", "thread_id": "thr-body", "from": "a@example.com",
               "subject": "payroll", "labels": []}
    assert store.admit(listing) == "admitted"
    assert store.admit({"message_id": "msg-turn", "thread_id": "thr-turn"}) == "admitted"
    store.store_turn("msg-turn", {"conversation_id": "thr-turn", "reply_ref": "msg-turn",
                                  "delivery_id": "msg-turn", "text": "stored, never posted"})
    assert store.admit({"message_id": "msg-done", "thread_id": "thr-done"}) == "admitted"
    store.store_turn("msg-done", {"conversation_id": "thr-done", "reply_ref": "msg-done"})
    store.accept_ingress("msg-done")
    store.close()
    with sqlite3.connect(path) as connection:
        connection.execute("ALTER TABLE completion_events DROP COLUMN send_attempted_at")
        connection.execute("DELETE FROM metadata WHERE key='provider'")
        connection.execute(
            "UPDATE deliveries SET summary_json=? WHERE message_id='msg-body'",
            (json.dumps(listing),),
        )
        connection.execute("PRAGMA user_version=3")


def test_the_schema_four_migration_settles_pending_rows_and_drops_raw_listings(
    tmp_path: Path,
) -> None:
    path = tmp_path / "v3.sqlite3"
    _schema_three_file(path)

    store = DurableMailState(str(path), max_pending=10, max_bytes=1 << 24)
    try:
        assert store.provider() == "agentmail"
        assert store.pending() == []
        assert store.delivery("msg-body") == {"state": "rejected", "turn": None}
        assert store.delivery("msg-turn") == {"state": "rejected", "turn": None}
        # A delivery accepted before keeps its reply ownership.
        assert store.delivery("msg-done")["state"] == "accepted"  # type: ignore[index]
        assert store.live_reply_refs("thr-turn") == []
        assert store.live_reply_refs("thr-done") == ["msg-done"]
        summaries = [
            json.loads(row[0])
            for row in store.connection.execute("SELECT summary_json FROM deliveries")
        ]
        assert all(set(summary) == {"message_id"} for summary in summaries)
        assert int(store.connection.execute("PRAGMA user_version").fetchone()[0]) == SCHEMA_VERSION
    finally:
        store.close()

    store = DurableMailState(str(path), max_pending=10, max_bytes=1 << 24)
    with pytest.raises(RuntimeError, match="belongs to mail provider 'agentmail'"):
        store.bind_provider("verifying-test")
    store.close()


def test_a_new_state_file_records_its_provider_on_first_boot(tmp_path: Path) -> None:
    store = DurableMailState(str(tmp_path / "new.sqlite3"), max_pending=10, max_bytes=1 << 24)
    try:
        assert store.provider() is None
        store.bind_provider("verifying-test")
        assert store.provider() == "verifying-test"
        store.bind_provider("verifying-test")
    finally:
        store.close()
