"""`Attachment`: the inbound attachment REFERENCE a `QueuedTurn` carries (#2567).

The model is deliberately a *reference*, never a payload. `id` is adapter-scoped
and opaque: the adapter that produced the turn is the only thing that can
resolve it, and the wire carries no `url`, no `download_url` and no bytes. That
omission is a decision under test here rather than an oversight -- the Slack bot
token has no `files:read` scope today, and
`apps/dispatcher/slack-app-manifest.yaml` states in its own comments (twice,
lines 24 and 41) that adding a scope requires reinstalling the app in every
workspace. A url field on the wire would therefore be a promise nothing in this
release can keep, and a reader who found one would reasonably assume it worked.

Policy is the one every ACI wire model shares (`_AciModel`,
`packages/aci-protocol/src/aci_protocol/events.py`): strict producers, tolerant
consumers. Both halves are asserted, and the consumer half is asserted through
the NESTED position an attachment actually occupies (inside
`QueuedTurn.attachments`), because a nested model is where reader-context
propagation has to reach -- the same claim `test_turn.py` makes for
`ReplyHandle`.
"""

import json

import pytest
from aci_protocol import Attachment, QueuedTurn, ReplyHandle, parse_queued_turn
from aci_protocol.events import READER_CONTEXT
from pydantic import ValidationError


def _attachment_payload(**overrides: object) -> dict[str, object]:
    """A minimal well-formed attachment payload, mutated by the caller.

    Built from one place so a test that removes `id` and a test that adds an
    unknown key cannot drift into asserting about two different base shapes.
    """
    payload: dict[str, object] = {"id": "F0EXAMPLE1", "name": "incident.pdf"}
    payload.update(overrides)
    return payload


def _turn_payload(attachments: list[dict[str, object]]) -> dict[str, object]:
    """A queued-turn payload carrying `attachments`, otherwise minimal."""
    return {
        "event_id": "Ev0ATTACH0001",
        "conversation_id": "1720000000.000100",
        "author": "U0EXAMPLE1",
        "text": "here is the incident report",
        "reply_handle": {"kind": "slack", "channel": "C0EXAMPLE1", "placeholder": "1.0"},
        "received_at": "2026-07-05T00:00:00+00:00",
        "attachments": attachments,
    }


# --- the required half: an unresolvable reference is refused at the source ----


@pytest.mark.parametrize("missing", ["id", "name"])
def test_attachment_requires_both_its_identifying_fields(missing: str) -> None:
    """`id` and `name` are REQUIRED and deliberately have no default.

    An attachment with no `id` is a reference that nothing can resolve, and an
    attachment with no `name` is one nothing can describe to the user. Either
    default would let a producer mint a ref that reads as present while carrying
    nothing actionable -- the "port with no caller" shape this issue complains
    about, reproduced one level down.
    """

    payload = _attachment_payload()
    del payload[missing]

    with pytest.raises(ValidationError):
        Attachment.model_validate(payload)


def test_a_missing_required_field_is_refused_even_by_a_tolerant_consumer() -> None:
    """Tolerance is about UNKNOWN fields, never about MISSING required ones.

    Same rule `test_turn.py` asserts for a kindless `ReplyHandle`: without this,
    `extra="ignore"` under the reader context could be mistaken for a licence to
    decode a half-built attachment, and the turn would reach the worker carrying
    a ref with no id.
    """

    payload = _turn_payload([_attachment_payload()])
    del payload["attachments"][0]["id"]  # type: ignore[index]

    with pytest.raises(ValidationError):
        QueuedTurn.model_validate(payload, context=READER_CONTEXT)


# --- the optional half --------------------------------------------------------


def test_the_descriptive_fields_are_optional_and_default_to_none() -> None:
    """`mime_type` and `size_bytes` are best-effort channel metadata.

    A channel that reports neither must still be able to produce a usable ref,
    so both default to None rather than being required or defaulted to a
    fabricated value like "application/octet-stream" or 0.
    """

    ref = Attachment(id="F0EXAMPLE1", name="incident.pdf")

    assert ref.mime_type is None
    assert ref.size_bytes is None


def test_the_model_carries_no_url_or_download_field() -> None:
    """The resolve seam is `id` + the producing adapter, and nothing else.

    Asserted as an absence because the absence is the decision (see the module
    docstring). A future change that adds a url field must fail here and be
    argued for, rather than arriving as an unremarked field on a wire model.
    """

    assert set(Attachment.model_fields) == {"id", "name", "mime_type", "size_bytes"}


# --- strict producers, tolerant consumers ------------------------------------


def test_constructing_an_attachment_with_an_unknown_field_is_strict() -> None:
    """A producer that invents a field is caught at the source.

    This is the half that keeps a well-meaning adapter from smuggling a
    `url_private` onto the wire under a name nothing else models.
    """

    with pytest.raises(ValidationError):
        Attachment(id="F0EXAMPLE1", name="incident.pdf", url_private="https://example.test/f")


def test_a_consumer_tolerates_an_unknown_key_inside_a_nested_attachment() -> None:
    """The reader context must reach `Attachment`, not just the turn above it.

    A newer producer that adds a field to the attachment shape must not make
    this consumer reject the whole turn -- that is the mid-deploy skew the
    tolerant-consumer policy exists for. Decoded through `parse_queued_turn`,
    the sanctioned consumer decode, so the test exercises the tolerance
    mechanism the queue boundary actually uses rather than passing the flag by
    hand.
    """

    payload = _turn_payload([_attachment_payload(future_attachment_field="from a newer image")])

    turn = parse_queued_turn(json.dumps(payload))

    assert len(turn.attachments) == 1
    assert turn.attachments[0].id == "F0EXAMPLE1"
    assert turn.attachments[0].name == "incident.pdf"
    assert not hasattr(turn.attachments[0], "future_attachment_field")


# --- the wire ----------------------------------------------------------------


def test_attachments_round_trip_through_the_queue_payload_encoding() -> None:
    """Refs survive the encode/decode pair the Valkey Stream carries them in.

    The Stream entry holds the turn as a single `payload` field
    (`STREAM_PAYLOAD_FIELD`), written with `model_dump_json` and read back with
    `parse_queued_turn` -- literally the body of the dispatcher's
    `to_stream_fields`/`from_stream_fields`
    (`apps/dispatcher/src/curie_dispatcher/queue.py`). That pair is driven
    against real Valkey in `apps/dispatcher/tests/test_inbound_attachments.py`;
    this test pins the model half of it, so a field that survives construction
    but not serialization fails here rather than in the dispatcher suite.
    """

    turn = QueuedTurn(
        event_id="Ev0ATTACH0001",
        conversation_id="1720000000.000100",
        author="U0EXAMPLE1",
        text="here is the incident report",
        reply_handle=ReplyHandle(kind="slack", channel="C0EXAMPLE1", placeholder="1.0"),
        received_at="2026-07-05T00:00:00+00:00",
        attachments=[
            Attachment(
                id="F0EXAMPLE1",
                name="incident.pdf",
                mime_type="application/pdf",
                size_bytes=12345,
            ),
            Attachment(id="F0EXAMPLE2", name="screenshot.png"),
        ],
    )

    restored = parse_queued_turn(turn.model_dump_json())

    assert restored == turn
    assert [ref.id for ref in restored.attachments] == ["F0EXAMPLE1", "F0EXAMPLE2"]
    assert restored.attachments[0].mime_type == "application/pdf"
    assert restored.attachments[0].size_bytes == 12345
    assert restored.attachments[1].mime_type is None
    assert restored.attachments[1].size_bytes is None
