"""Boot gates: the process refuses to run half-configured, and its health path.

Driven through the real `python -m curie_mail_adapter` entry point in a
subprocess with a closed-world environment, because "exits non-zero naming the
variable" is a property of the process, not of a function. The adapter's inbox is
a public mailbox by construction, so an install that comes up with no allow-list
is an unauthenticated trigger for agent turns available to anyone who learns the
address; crashing at boot is the same shape the dispatcher already uses for its
own unconfigurable-to-run state.
"""

from __future__ import annotations

import json
from collections.abc import Callable

import pytest
from _support import (
    INBOX,
    IngressState,
    MailState,
    adapter_env,
    exit_of,
    free_port,
    get,
    post_raw,
    spawn_adapter,
    stop,
    wait_for_healthz,
)
from curie_mail_adapter.adapter import MailAdapter


def test_ingress_enabled_with_an_empty_allow_list_refuses_to_boot(
    mail: MailState, ingress: IngressState
) -> None:
    """Empty means deny everything, and it is refused rather than served as deny-all.

    Fail-open here would make every first-party email install an open trigger from
    the moment it is enabled, with whatever tool surface the bound agent carries.
    """
    proc = spawn_adapter(
        adapter_env(
            agentmail_base_url=mail.base_url,
            api_url=ingress.url,
            port=free_port(),
            allowed_senders="",
        )
    )

    code, output = exit_of(proc)

    assert code != 0
    assert "CURIE_MAIL_ALLOWED_SENDERS" in output


def test_ingress_disabled_with_an_empty_allow_list_boots(
    mail: MailState, ingress: IngressState
) -> None:
    """The staged cutover the guide documents stays available."""
    port = free_port()
    proc = spawn_adapter(
        adapter_env(
            agentmail_base_url=mail.base_url,
            api_url=ingress.url,
            port=port,
            ingress_enabled="false",
            allowed_senders="",
        )
    )
    try:
        wait_for_healthz(port, proc)

        assert proc.poll() is None
    finally:
        stop(proc)


@pytest.mark.parametrize(
    "missing",
    ["AGENTMAIL_INBOX", "AGENTMAIL_API_KEY", "CURIE_CHANNEL_TOKEN", "CURIE_EGRESS_SECRET"],
)
def test_a_missing_credential_refuses_to_boot(
    mail: MailState, ingress: IngressState, missing: str
) -> None:
    """A pod that runs without one of these 401s behind a green health probe."""
    env = adapter_env(agentmail_base_url=mail.base_url, api_url=ingress.url, port=free_port())
    del env[missing]

    code, output = exit_of(spawn_adapter(env))

    assert code != 0
    assert missing in output


@pytest.mark.parametrize("value", ["0", "-1"])
def test_a_non_positive_poll_interval_refuses_to_boot(
    mail: MailState, ingress: IngressState, value: str
) -> None:
    """Otherwise a chart typo is a tight loop hammering a third-party API."""
    env = adapter_env(
        agentmail_base_url=mail.base_url,
        api_url=ingress.url,
        port=free_port(),
        poll_interval_seconds=value,
    )

    code, output = exit_of(spawn_adapter(env))

    assert code != 0
    assert "CURIE_MAIL_POLL_INTERVAL_SECONDS" in output


def test_healthz_is_open_to_get_and_closed_to_post(
    adapter: MailAdapter, serve_egress: Callable[[MailAdapter], str]
) -> None:
    """The second half is the one that matters: a probe path must not become an
    unauthenticated write. `do_POST` must not special-case the path, and the GET
    body must reveal nothing about the install.
    """
    base = serve_egress(adapter)

    status, body = get(f"{base}/healthz")

    assert status == 200
    assert INBOX not in json.dumps(body)

    status, _ = post_raw(f"{base}/healthz", secret=None)

    assert status == 401
