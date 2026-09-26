"""The binding schemas store a Slack route's identity by name (ADR-0168 decision 3).

A Slack route names its identity in `adapter` -- `default` when omitted --
and carries no endpoint; the pre-ADR custom-transport form is refused by
field name. A row an older writer left NULL still reads as `default`.
"""

import pytest
from curie_api.models import AgentChannel
from curie_api.schemas import (
    ApprovalNotificationTarget,
    ChannelBindingOut,
    ChannelBindingPatch,
    ChannelBindingWrite,
)
from pydantic import ValidationError


def test_a_slack_write_stores_the_default_identity_by_name() -> None:
    assert ChannelBindingWrite(kind="slack", address="C0EXAMPLE1").adapter == "default"
    explicit = ChannelBindingWrite(kind="slack", address="C0EXAMPLE1", adapter="default")
    assert explicit.adapter == "default"


def test_an_omitted_slack_adapter_is_not_marked_as_explicitly_sent() -> None:
    """A PATCH reads `model_fields_set` to decide whether a route was touched,
    so resolving the omitted identity must not mark it as sent."""

    omitted = ChannelBindingPatch(kind="slack", address="C0EXAMPLE1")
    assert "adapter" not in omitted.model_fields_set
    assert omitted.adapter == "default"
    sent = ChannelBindingPatch(kind="slack", address="C0EXAMPLE1", adapter="default")
    assert "adapter" in sent.model_fields_set


@pytest.mark.parametrize("adapter", [None, "proof-offline", "default"])
def test_a_slack_endpoint_is_refused_by_field_name(adapter: str | None) -> None:
    with pytest.raises(ValidationError, match="a Slack route takes no endpoint"):
        ChannelBindingWrite(
            kind="slack", address="C0EXAMPLE1", endpoint="http://127.0.0.1:1", adapter=adapter
        )


def test_a_slack_endpoint_refusal_never_echoes_the_endpoint() -> None:
    with pytest.raises(ValidationError) as refused:
        ChannelBindingWrite(
            kind="slack", address="C0EXAMPLE1", endpoint="http://secret-token.test/", adapter="x"
        )
    assert "secret-token" not in str(refused.value)


def test_a_slack_patch_with_an_endpoint_is_refused() -> None:
    with pytest.raises(ValidationError, match="a Slack route takes no endpoint"):
        ChannelBindingPatch(kind="slack", address="C0EXAMPLE2", endpoint="http://127.0.0.1:1")


def test_an_undeclared_slack_identity_is_refused_by_name() -> None:
    with pytest.raises(ValidationError, match="'second'.*default"):
        ChannelBindingWrite(kind="slack", address="C0EXAMPLE1", adapter="second")


def test_a_non_slack_route_stays_both_or_neither() -> None:
    with pytest.raises(ValidationError, match="half-configured"):
        ChannelBindingWrite(kind="email", address="a@example.com", adapter="agentmail")
    assert ChannelBindingWrite(kind="email", address="a@example.com").adapter is None


def test_the_read_side_names_the_identity_and_never_the_endpoint() -> None:
    out = ChannelBindingOut(kind="slack", address="C0EXAMPLE1", adapter="default")
    assert out.model_dump() == {"kind": "slack", "address": "C0EXAMPLE1", "adapter": "default"}


def test_a_row_an_older_writer_left_null_reads_as_the_default_identity() -> None:
    row = AgentChannel(kind="slack", address="C0EXAMPLE1", adapter=None)
    assert ChannelBindingOut.model_validate(row).adapter == "default"


def test_a_non_slack_row_with_no_identity_stays_none_on_read() -> None:
    assert ChannelBindingOut(kind="email", address="a@example.com", adapter=None).adapter is None


def test_a_slack_notification_keeps_the_default_identity_implicit() -> None:
    """A notification target is a stored JSON document, not a route row: the
    default stays implicit so stored routes, and comparisons of them, do not move."""

    for target in (
        ApprovalNotificationTarget(kind="slack", address="C0EXAMPLE3"),
        ApprovalNotificationTarget(kind="slack", address="C0EXAMPLE3", adapter="default"),
    ):
        assert target.adapter is None
        assert "adapter" not in target.model_dump()


def test_a_slack_notification_endpoint_is_refused() -> None:
    with pytest.raises(ValidationError, match="a Slack route takes no endpoint"):
        ApprovalNotificationTarget(
            kind="slack", address="C0EXAMPLE3", endpoint="http://127.0.0.1:1", adapter="x"
        )


def test_the_reserved_relay_adapter_wins_over_the_undeclared_identity_message() -> None:
    """`curie-cluster-message` is refused as RESERVED, not as an undeclared
    Slack identity -- the more specific, more actionable answer, checked
    before the declared-identity set even though the literal also fails
    that check."""

    with pytest.raises(ValidationError, match="reserved"):
        ChannelBindingWrite(kind="slack", address="C0EXAMPLE1", adapter="curie-cluster-message")
