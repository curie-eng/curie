"""A 3xx is a delivery failure, and a credential is never replayed at its target.

`docs/guides/building-a-channel-adapter.md` section 4 states the rule the whole
adapter pattern is built on: "Never redirect. A 3xx is treated as a delivery
failure and is not followed, because following it would replay the egress secret
at whatever origin the redirect named", restated in the section 7 conformance
floor as "never redirect".

`urllib.request.urlopen` does the opposite by default: its redirect handler
rebuilds the request for the new URL carrying every header the caller added,
content headers excepted, so one 302 from a compromised or misconfigured origin
hands out `AGENTMAIL_API_KEY` on the provider calls and `CURIE_CHANNEL_TOKEN` on
the ingress call. The redirect target here is a real local HTTP server that
records what reached it, so the leak is asserted as data rather than inferred
from the shape of the code.
"""

from __future__ import annotations

from collections.abc import Callable

import pytest
from _support import MailState, RedirectState, SinkState
from curie_mail_adapter.adapter import MailAdapter
from curie_mail_adapter.agentmail import AgentMailClient
from curie_mail_adapter.config import MailAdapterConfig


@pytest.mark.parametrize("status", [301, 302, 303, 307, 308])
def test_a_redirected_provider_call_never_reaches_the_named_origin(
    redirect: RedirectState,
    sink: SinkState,
    make_config: Callable[..., MailAdapterConfig],
    status: int,
) -> None:
    """Every 3xx, driven through the real client so the header is the real one.

    307 and 308 are the sharp ones: they preserve the method and the body as
    well as the headers, so the leak is the complete original request.
    """
    redirect.status = status
    client = AgentMailClient(make_config(agentmail_base_url=redirect.url))

    list_status, _ = client.list_messages(20)

    assert sink.credentials_seen() == [], "the API key was replayed at the redirect target"
    assert sink.headers == []
    assert list_status != 200, "a redirect must not be reported as a successful listing"
    assert redirect.hits == 1


def test_a_redirected_ingress_post_never_replays_the_channel_token(
    mail: MailState,
    redirect: RedirectState,
    sink: SinkState,
    make_adapter: Callable[..., MailAdapter],
) -> None:
    """The same hole on the platform side, and the more valuable credential.

    `CURIE_CHANNEL_TOKEN` authorizes starting agent turns on the binding, so an
    origin that captures it can drive the agent directly.
    """
    adapter = make_adapter(api_base_url=redirect.url)
    mail.add_inbound("msg-1", "thr-1")

    adapter.poll_once()

    assert sink.credentials_seen() == [], "the channel token was replayed at the redirect target"
    # The name-independent form of the same claim: nothing at all arrived, so
    # this cannot pass by watching for a header the adapter has since renamed.
    assert sink.headers == []
    assert redirect.hits >= 1, "the redirecting origin was never reached"
