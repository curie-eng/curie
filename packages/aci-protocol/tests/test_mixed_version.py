"""T-A15 / AC10d: an old (pre-0.3.0) consumer provably DROPS `kind`.

This is the executable form of the hazard that orders the cutover (plan section
6.1, step 3 before migration 0023). The claim under test is not "old pods are
stale"; it is that an old pod reading a NEW turn keeps routing on `address`
alone, and once 0023 permits two kinds to share one address, that is a silent
misroute to the wrong agent -- not a dead-letter, not an error, not anything an
operator would see.

Why a pinned fixture and a re-declared model rather than a live old pod: the
0.2.x model is what has to be held still. A test that imported the current
`ReplyHandle` would silently start asserting about the NEW shape the moment the
field lands, and go green while proving nothing. So the 0.2.x shape is committed
as `fixtures/reply_handle_0_2_9.json` and re-declared here (three fields, the
same `_AciModel` base the real 0.2.9 model used), and the fixture is what proves
the re-declaration is faithful.

Mutation that must fail this test: change `_ReplyHandle_0_2_9`'s config to
`extra="forbid"` and the first assertion fails -- which is the point. The
tolerance (`events.py:38-48`, `extra="ignore"` for consumers) is exactly what
creates the hazard: the old pod does not reject the new payload, it accepts it
minus the routing half.
"""

import json
from pathlib import Path
from typing import Literal

from aci_protocol import PROTOCOL_VERSION, Event, ReplyHandle, is_compatible, parse_inbound
from aci_protocol.events import READER_CONTEXT, PublicationContext, ToolAccess, _AciModel
from pydantic import Field

_FIXTURE = Path(__file__).resolve().parent / "fixtures" / "reply_handle_0_2_9.json"


class _ReplyHandle_0_2_9(_AciModel):  # noqa: N801 - the version IS the name
    """The `ReplyHandle` shape as of PROTOCOL_VERSION 0.2.9, pinned.

    Re-declared rather than imported on purpose: importing the live model would
    make this test follow the change it exists to measure. It inherits
    `_AciModel` so it carries the SAME reader-context/`extra="ignore"` policy the
    real 0.2.9 model carried -- the policy is the mechanism under test.
    """

    channel: str
    placeholder: str
    endpoint: str | None = None


def test_the_pinned_model_matches_the_committed_0_2_9_payload() -> None:
    """The re-declaration is faithful to the shape that actually shipped.

    Without this, the rest of the file could be asserting about a model nobody
    ever ran.
    """

    payload = json.loads(_FIXTURE.read_text())
    assert set(payload) == {"channel", "placeholder", "endpoint"}

    old = _ReplyHandle_0_2_9.model_validate(payload, context=READER_CONTEXT)
    assert old.channel == "C0GOLDENAAA"
    assert old.placeholder == "1720000000.000200"
    assert old.endpoint is None


def test_the_0_2_9_model_parses_a_0_3_0_payload_and_loses_the_kind() -> None:
    """T-A15, both halves.

    (a) Parsing SUCCEEDS: `extra="ignore"` under the reader context tolerates the
        new field, so nothing anywhere tells the old consumer it is out of date.
    (b) The parsed object has NO `kind` at all, so a resolver fed from it can
        only route on `address` -- the silent misroute, made executable.

    The 0.3.0 payload is built from the LIVE model rather than hand-written, so
    the test cannot drift from whatever shape the contract actually ships.
    """

    new_payload = json.loads(
        ReplyHandle(
            kind="email",
            channel="agent@example.test",
            placeholder="msg_abc123",
            endpoint="http://curie-mail-adapter:8080/",
            adapter="agentmail-sandbox",
        ).model_dump_json()
    )
    assert new_payload["kind"] == "email", "the 0.3.0 payload must carry the new field"

    old = _ReplyHandle_0_2_9.model_validate(new_payload, context=READER_CONTEXT)

    # (a) It parsed. No exception, no warning, no version gate -- `QueuedTurn`
    # has no `version` field at all (`ndjson.py:109-118`), which is why FU-7
    # exists and why this cutover is quiescent.
    assert old.channel == "agent@example.test"

    # (b) And the routing half is simply gone.
    assert not hasattr(old, "kind")
    assert "kind" not in _ReplyHandle_0_2_9.model_fields
    assert "kind" not in old.model_dump()
    assert "adapter" not in old.model_dump()


def test_an_old_consumer_cannot_distinguish_two_kinds_at_one_address() -> None:
    """T-A15's consequence stated as behavior, not as prose.

    After migration 0023 widens uniqueness to `(kind, address)`, an email turn
    and a Slack turn can legitimately name the same address. To an old consumer
    the two payloads are INDISTINGUISHABLE, so whichever agent its address-only
    predicate finds first answers both. That is the misroute; asserting the
    equality is asserting the ambiguity.
    """

    # Identical in every field the 0.2.9 model MODELS, differing only in `kind`.
    # That isolation is the point: `endpoint` is carried by both versions, so two
    # turns differing in it would fail this assertion for a reason that has
    # nothing to do with the kind-dropping tolerance under test.
    shared_address = "C0EXAMPLE1"
    slack_turn = json.loads(
        ReplyHandle(kind="slack", channel=shared_address, placeholder="1.0").model_dump_json()
    )
    email_turn = json.loads(
        ReplyHandle(kind="email", channel=shared_address, placeholder="1.0").model_dump_json()
    )
    assert slack_turn != email_turn, "the two 0.3.0 payloads must differ, and differ in kind"
    assert slack_turn["kind"] != email_turn["kind"]

    as_seen_by_old_slack = _ReplyHandle_0_2_9.model_validate(slack_turn, context=READER_CONTEXT)
    as_seen_by_old_email = _ReplyHandle_0_2_9.model_validate(email_turn, context=READER_CONTEXT)

    assert as_seen_by_old_slack.channel == as_seen_by_old_email.channel
    assert as_seen_by_old_slack.model_dump() == as_seen_by_old_email.model_dump(), (
        "an old consumer sees the two turns as the same routing key; only the "
        "cutover ordering (no old worker before 0023) prevents the misroute"
    )


# --- Event.memory_token (0.5.12) ----------------------------------------------
#
# The other direction from the 0.2.9 hazard above: here dropping the field is the
# intended behaviour. A 0.5.11 runner must ignore the credential (and fall back to
# its env token), and a 0.5.11 worker's event, which never carries it, must
# decode on a 0.5.12 runner as a turn with no write credential.


class _Event_0_5_11(_AciModel):  # noqa: N801 - the version IS the name
    """The `Event` shape as of PROTOCOL_VERSION 0.5.11, pinned (no `memory_token`)."""

    kind: Literal["event"] = "event"
    type: Literal["message", "job", "eval_case"]
    text: str
    user: str
    ts: str
    session_id: str | None = None
    history_ref: str | None = None
    publication_context: PublicationContext | None = None
    tool_access: ToolAccess | None = None


def _event_0_5_11() -> dict[str, object]:
    """A 0.5.11 worker's event: every 0.5.11 field, and no `memory_token` key."""

    return {
        "kind": "event",
        "type": "message",
        "text": "Remember that the standup moved to 10am",
        "user": "U0EXAMPLE1",
        "ts": "1720000000.000100",
        "session_id": None,
        "history_ref": None,
        "publication_context": None,
        "tool_access": None,
    }


def test_the_pinned_0_5_11_event_matches_the_live_model_minus_memory_token() -> None:
    """The pinned reader predates the two optional credential fields."""

    assert set(Event.model_fields) - set(_Event_0_5_11.model_fields) == {
        "memory_token",
        "channel_read",
    }
    assert set(_Event_0_5_11.model_fields) <= set(Event.model_fields)


def test_a_0_5_11_event_without_memory_token_decodes_with_null() -> None:
    payload = _event_0_5_11()
    assert "memory_token" not in payload

    consumed = parse_inbound(json.dumps(payload))

    assert isinstance(consumed, Event)
    assert consumed.memory_token is None
    assert consumed.text == payload["text"]


def test_an_event_carrying_memory_token_decodes_under_the_0_5_gate() -> None:
    # 0.5.11 and 0.5.12 share major.minor, so neither side refuses the other.
    assert is_compatible("0.5.11", PROTOCOL_VERSION) is True
    assert is_compatible(PROTOCOL_VERSION, "0.5.11") is True

    new_payload = json.loads(
        Event(
            type="message",
            text="Remember that the standup moved to 10am",
            user="U0EXAMPLE1",
            ts="1720000000.000100",
            memory_token="sbx.eyJhZ2VudCI6ImFjbWUtYm90In0.not-a-real-signature",
        ).model_dump_json()
    )
    assert new_payload["memory_token"] is not None

    # The new runner reads it.
    consumed = parse_inbound(json.dumps(new_payload))
    assert isinstance(consumed, Event)
    assert consumed.memory_token == new_payload["memory_token"]

    # An old runner parses the same payload and simply has no credential.
    old = _Event_0_5_11.model_validate(new_payload, context=READER_CONTEXT)
    assert old.text == new_payload["text"]
    assert "memory_token" not in old.model_dump()


class _Event_0_5_15(_Event_0_5_11):  # noqa: N801
    """The previous event shape contains memory credentials but no channel read."""

    memory_token: str | None = Field(default=None, repr=False)


def test_pinned_0_5_15_event_has_exactly_the_previous_patch_fields() -> None:
    assert set(Event.model_fields) - set(_Event_0_5_15.model_fields) == {"channel_read"}
    assert set(_Event_0_5_15.model_fields) <= set(Event.model_fields)
    assert is_compatible("0.5.15", PROTOCOL_VERSION)
    assert is_compatible(PROTOCOL_VERSION, "0.5.15")


def test_previous_patch_event_without_channel_read_decodes_without_capability() -> None:
    previous = _Event_0_5_15.model_validate(_event_0_5_11())
    payload = previous.model_dump_json()
    assert "channel_read" not in previous.model_dump()
    current = parse_inbound(payload)
    assert isinstance(current, Event)
    assert current.channel_read is None
    assert current.text == previous.text
    assert current.user == previous.user


def test_previous_patch_reader_accepts_and_drops_optional_channel_read_object() -> None:
    current = Event.model_validate(
        {
            **_event_0_5_11(),
            "memory_token": "memory.example.signature",
            "channel_read": {
                "url": "https://api.example.com/channel-read",
                "token": "channel.read.example.signature",
            },
        }
    )
    payload = json.loads(current.model_dump_json())
    assert payload["channel_read"]["token"] == "channel.read.example.signature"
    previous = _Event_0_5_15.model_validate(payload, context=READER_CONTEXT)
    assert previous.text == current.text
    assert previous.memory_token == "memory.example.signature"
    assert "channel_read" not in previous.model_dump()
    assert not hasattr(previous, "channel_read")
