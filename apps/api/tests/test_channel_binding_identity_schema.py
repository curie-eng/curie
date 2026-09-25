"""The binding write/read schemas become kind-aware for identity (ADR-0168 D3).

The stored form does not change -- a Slack route with no identity is still
stored as NULL, exactly as it was before ADR-0168. The PRE-ADR-0168
custom-transport form is kept too: a Slack route that carries an endpoint is
not an identity at all, and its `adapter` is an egress credential slug like
any other kind's (the hook-approval-proof rig in
`charts/curie/ci/hook-approval-proof.py` is exactly this shape, and must not
422). Only a Slack route WITHOUT an endpoint names an identity: `adapter` may
be omitted or `"default"`, is checked against the identities this
installation declares, and a stored NULL reads back as `"default"`. The
contract migration for ADR-0168 decision 3 (#3100) refuses a Slack endpoint
outright and flips the stored form; until it lands, this file pins the
unchanged form.
"""

import pytest
from curie_api.models import AgentChannel
from curie_api.schemas import ChannelBindingOut, ChannelBindingPatch, ChannelBindingWrite
from pydantic import ValidationError


def test_a_slack_write_of_the_default_identity_keeps_the_stored_form() -> None:
    """The stored form is unchanged: both the omitted and the explicit
    `"default"` spelling normalize to the pre-ADR-0168 stored form (None), so an
    older app reading this row back still sees exactly what it always wrote."""

    assert ChannelBindingWrite(kind="slack", address="C0EXAMPLE1").adapter is None
    assert (
        ChannelBindingWrite(kind="slack", address="C0EXAMPLE1", adapter="default").adapter is None
    )


def test_an_omitted_slack_adapter_is_not_marked_as_explicitly_sent() -> None:
    """The default-identity normalization writes the stored form directly,
    without going through `self.adapter = ...`, because that setattr would
    mark `adapter` as SENT even when the caller omitted it -- and a PATCH
    reads `model_fields_set` to decide whether a route was touched at all."""

    omitted = ChannelBindingPatch(kind="slack", address="C0EXAMPLE1")
    assert "adapter" not in omitted.model_fields_set
    assert omitted.adapter is None

    sent = ChannelBindingPatch(kind="slack", address="C0EXAMPLE1", adapter="default")
    assert "adapter" in sent.model_fields_set
    assert sent.adapter is None


def test_a_slack_route_with_an_endpoint_keeps_the_old_custom_transport_form() -> None:
    """A Slack endpoint is not refused -- ADR-0168 decision 3 retires this form
    only in its contract migration (#3100). `adapter` here is a CREDENTIAL
    slug, not an identity: it is stored exactly as sent and never checked
    against the declared-identity set. This is the hook-approval-proof rig's shape."""

    written = ChannelBindingWrite(
        kind="slack",
        address="C0EXAMPLE1",
        endpoint="http://127.0.0.1:1",
        adapter="proof-offline",
    )
    assert written.endpoint == "http://127.0.0.1:1"
    assert written.adapter == "proof-offline"


def test_a_slack_endpoint_without_an_adapter_is_half_configured() -> None:
    """The custom-transport form is still both-or-neither: an endpoint alone
    is exactly as unroutable as it is for any other kind."""

    with pytest.raises(ValidationError, match="half-configured"):
        ChannelBindingWrite(kind="slack", address="C0EXAMPLE1", endpoint="http://127.0.0.1:1")


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


def test_a_stored_null_slack_row_reads_as_the_default_identity() -> None:
    """The pre-ADR-0168 stored form (NULL) is what stays on the row --
    kept so an older app reads what it wrote -- and the read side is what
    presents it as the identity it always meant."""

    assert ChannelBindingOut(kind="slack", address="C0EXAMPLE1", adapter=None).adapter == "default"


def test_a_stored_null_slack_row_from_an_orm_object_reads_as_the_default_identity() -> None:
    """The same normalization, built the way a real read actually builds it: a
    `from_attributes` validation off an `AgentChannel` row, with no DB involved
    -- the after-validator must not assume keyword construction."""

    row = AgentChannel(kind="slack", address="C0EXAMPLE1", adapter=None)
    out = ChannelBindingOut.model_validate(row)
    assert out.adapter == "default"


def test_a_non_slack_row_with_no_identity_stays_none_on_read() -> None:
    """A route-less non-Slack row is not an identity; `route_identity` leaves it
    unchanged, and the read side must not invent one for it."""

    out = ChannelBindingOut(kind="email", address="a@example.com", adapter=None)
    assert out.adapter is None


def test_a_slack_patch_may_send_the_identity_alone() -> None:
    """Moving a Slack binding names its identity without an endpoint. The
    explicitly sent default identity is stored as NULL, and still counts as
    SENT, which is what tells `crud.update_channel_binding` the route moved."""

    patch = ChannelBindingPatch(kind="slack", address="C0EXAMPLE2", adapter="default")
    assert patch.adapter is None
    assert "adapter" in patch.model_fields_set


def test_a_slack_patch_with_an_endpoint_still_needs_the_pair() -> None:
    """The identity-alone exception is for the identity form only; naming an
    endpoint still means the custom-transport form, which is still both-or-
    neither (caught by `ChannelBindingWrite._check_route`, which runs first)."""

    with pytest.raises(ValidationError, match="half-configured"):
        ChannelBindingPatch(
            kind="slack", address="C0EXAMPLE2", endpoint="http://127.0.0.1:1"
        )


def test_the_reserved_relay_adapter_wins_over_the_undeclared_identity_message() -> None:
    """`curie-cluster-message` is refused as RESERVED, not as an undeclared
    Slack identity -- the more specific, more actionable answer, checked
    before the declared-identity set even though the literal also fails
    that check."""

    with pytest.raises(ValidationError, match="reserved"):
        ChannelBindingWrite(kind="slack", address="C0EXAMPLE1", adapter="curie-cluster-message")
