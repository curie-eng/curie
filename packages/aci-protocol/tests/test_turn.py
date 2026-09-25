"""Reader-context propagation into nested models on the queued-turn payload,
and ``ReplyHandle``'s routing half (ADR-0096 phase 2, D1/D2).

``QueuedTurn`` crosses the Valkey Stream boundary between the dispatcher and the
worker, carrying a nested ``ReplyHandle``. The tolerant-read policy must reach
that nested model, not just the top-level one -- pydantic propagates validation
context into nested models, but decision 1 says assert it, do not assume it.

``ReplyHandle.kind`` is REQUIRED (plan section 1, D1): an optional kind forces
the resolver to answer "what do I do when kind is None?", and every honest answer
is an address-only fallback or a ``"slack"`` default -- #38's silent misroute
re-opened at the exact point the field that prevents it finally exists.
``ReplyHandle.adapter`` is optional at the schema (a third-party producer is not
rejected outright) but every first-party mint site sets it explicitly; the mint
sites are asserted by T-A17 (dispatcher), T-A18 (resume) and T-C2 (ingress).
"""

import json
from dataclasses import dataclass
from pathlib import Path

import pytest
from aci_protocol import (
    PROTOCOL_VERSION,
    HookRunRef,
    QueuedTurn,
    ReplyHandle,
    TurnSource,
    is_compatible,
    parse_queued_turn,
)
from aci_protocol.events import _READER_CONTEXT_KEY
from aci_protocol.turn import DEFAULT_IDENTITY, SLACK_KIND, matching_routes, route_identity
from pydantic import ValidationError

# The committed cross-language golden the Rust CLI re-serializes byte-identically
# (`cli/src/queue.rs::queued_turn_matches_cross_language_golden`, T-A3). Read from
# disk rather than inlined, so this test and that one cannot drift apart.
_GOLDEN = (
    Path(__file__).resolve().parents[1] / "schema" / "queued-turn.fixture.json"
)


def _turn_payload_with_unknown_fields() -> dict[str, object]:
    return {
        "event_id": "e1",
        "conversation_id": "c1",
        "author": "u1",
        "text": "hi",
        "received_at": "2026-01-01T00:00:00Z",
        "future_top": 1,
        "reply_handle": {
            "kind": "slack",
            "channel": "C1",
            "placeholder": "1.0",
            "future_nested": 2,
        },
    }


def test_reader_context_reaches_nested_reply_handle() -> None:
    # With the reader context, an unknown field on the TOP-LEVEL turn and an
    # unknown field on the NESTED reply handle are both ignored -- proving the
    # context propagated into ReplyHandle.
    turn = QueuedTurn.model_validate(
        _turn_payload_with_unknown_fields(),
        context={_READER_CONTEXT_KEY: True},
    )
    assert turn.event_id == "e1"
    assert turn.reply_handle.channel == "C1"


def test_nested_reply_handle_is_strict_without_reader_context() -> None:
    # Producers stay strict: without the reader context, the unknown nested field
    # is rejected on construction/validation.
    with pytest.raises(ValidationError):
        QueuedTurn.model_validate(_turn_payload_with_unknown_fields())


def test_parse_queued_turn_tolerates_unknown_top_level_and_nested_fields() -> None:
    # The sanctioned consumer decode threads the reader context, so an unknown
    # field on the TOP-LEVEL turn and an unknown field on the NESTED reply handle
    # are both tolerated (the queue-boundary counterpart to the NDJSON decoders).
    turn = parse_queued_turn(json.dumps(_turn_payload_with_unknown_fields()))
    assert turn.event_id == "e1"
    assert turn.reply_handle.channel == "C1"


def test_constructing_queued_turn_with_unknown_field_is_strict() -> None:
    # Tolerance is read-only: producers construct the model directly, where an
    # unknown field is still rejected.
    with pytest.raises(ValidationError):
        QueuedTurn(
            event_id="e1",
            conversation_id="c1",
            author="u1",
            text="hi",
            reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="1.0"),
            received_at="2026-01-01T00:00:00Z",
            bogus=1,
        )


# --- T-A1: the routing half of the pair is required ---------------------------


def test_reply_handle_without_kind_is_rejected_on_construction() -> None:
    """T-A1 / AC1. A producer that omits `kind` is refused at the source.

    This is the assertion that forecloses the compatibility path: with `kind`
    optional, `ReplyHandle(channel=..., placeholder=...)` would construct fine
    and the resolver would have to invent a kind for it.
    """

    with pytest.raises(ValidationError):
        ReplyHandle(channel="C1", placeholder="1.0")


def test_a_kindless_payload_is_rejected_even_by_a_tolerant_consumer() -> None:
    """T-A1, the consumer half. Tolerance is about UNKNOWN fields, never about
    MISSING required ones: an old (0.2.x) producer's turn arriving at a 0.3.0
    worker must dead-letter loudly (plan section 6.4, E1), not decode with a
    fabricated kind. Without this, `extra="ignore"` plus a defaulted `kind`
    would let the pre-cutover shape through silently.
    """

    payload = _turn_payload_with_unknown_fields()
    handle = dict(payload["reply_handle"])  # type: ignore[arg-type]
    handle.pop("kind")
    payload["reply_handle"] = handle

    with pytest.raises(ValidationError):
        QueuedTurn.model_validate(payload, context={_READER_CONTEXT_KEY: True})


def test_the_committed_golden_fixture_carries_slack_as_its_kind() -> None:
    """T-A1, the golden half / AC1 and AC6.

    `schema/queued-turn.fixture.json` is the cross-language golden: the Rust CLI
    asserts byte-identical re-serialization of this exact file (T-A3), so it is
    regenerated with the model, never hand-edited. Reading it back through the
    sanctioned consumer decode proves the regeneration happened AND that the new
    field landed with the only honest value for a Slack-shaped fixture.
    """

    turn = parse_queued_turn(_GOLDEN.read_text())

    assert turn.reply_handle.kind == "slack"
    assert turn.reply_handle.channel == "C0GOLDENAAA"
    assert turn.reply_handle.placeholder == "1720000000.000200"


def test_a_payload_written_before_source_existed_decodes_as_a_message() -> None:
    """The claim that makes this a PATCH rather than a breaking minor.

    Adding `source` was classed as a new optional field, which means a producer
    from the previous image must keep decoding on a consumer from this one. That
    is exactly what happens across a rolling deploy, and it is the whole reason
    the version gate stays quiet, so it is asserted rather than assumed.

    The default also has to be the NON-JOB value. If an unset source read as a
    job, a mid-deploy dispatcher would silently start minting turns the kernel
    refuses to steer, and every follow-up in a live thread would defer instead of
    joining the conversation.
    """

    pre_upgrade = json.loads(_GOLDEN.read_text())
    del pre_upgrade["source"]

    turn = QueuedTurn.model_validate(pre_upgrade, context={_READER_CONTEXT_KEY: True})

    assert turn.source is TurnSource.SLACK
    assert turn.source.is_job is False


def test_source_round_trips_and_a_job_is_distinguishable() -> None:
    """A job survives the wire and answers the one question the kernel asks."""

    payload = json.loads(_GOLDEN.read_text())
    payload["source"] = "cron"

    turn = QueuedTurn.model_validate(payload, context={_READER_CONTEXT_KEY: True})

    assert turn.source is TurnSource.CRON
    assert turn.source.is_job is True
    assert TurnSource.WEBHOOK.is_job is True
    assert json.loads(turn.model_dump_json())["source"] == "cron"


def test_an_unknown_source_is_refused_never_degraded() -> None:
    """`source` is control-bearing, so an unrecognized value is rejected.

    Same rule as `SessionStatus` (ADR-0036). Degrading an unknown value to the
    default would silently reclassify a future job kind as a person's message and
    let it steer a live session, which is the one thing this field exists to stop.
    """

    payload = json.loads(_GOLDEN.read_text())
    payload["source"] = "carrier-pigeon"

    with pytest.raises(ValidationError):
        QueuedTurn.model_validate(payload, context={_READER_CONTEXT_KEY: True})


def test_adapter_is_optional_and_round_trips_when_a_producer_sets_it() -> None:
    """T-A1, the `adapter` half (plan EB-A1, revision 4).

    `adapter` is the egress-credential selector a pre-resolution sink call needs
    (EB-B1's trust argument), so it must survive the wire, and it must be
    OPTIONAL at the schema so a third-party producer is not rejected outright.
    Both directions are asserted here: absent means None, present round-trips.
    """

    default = ReplyHandle(kind="slack", channel="C1", placeholder="1.0")
    assert default.adapter is None
    assert default.endpoint is None

    routed = ReplyHandle(
        kind="email",
        channel="agent@example.test",
        placeholder="msg_abc123",
        endpoint="http://curie-mail-adapter:8080/",
        adapter="agentmail-sandbox",
    )
    restored = ReplyHandle.model_validate_json(routed.model_dump_json())
    assert restored == routed
    assert restored.adapter == "agentmail-sandbox"
    assert restored.endpoint == "http://curie-mail-adapter:8080/"


def test_reply_handle_without_placeholder_raises() -> None:
    with pytest.raises(ValidationError):
        ReplyHandle.model_validate({"kind": "slack", "channel": "C1"})


def test_reply_handle_with_null_placeholder_succeeds() -> None:
    handle = ReplyHandle.model_validate(
        {"kind": "slack", "channel": "C1", "placeholder": None}
    )
    assert handle.placeholder is None


def test_reply_handle_with_empty_placeholder_succeeds() -> None:
    handle = ReplyHandle(kind="slack", channel="C1", placeholder="")
    assert handle.placeholder == ""


# --- #2567: optional attachment refs are a PATCH ------------------------------


def test_the_attachments_field_is_a_patch_bump() -> None:
    """ADR-0036's change-class table applied to this change, stated as a number.

    `attachments` is a NEW OPTIONAL FIELD with a default, so it is a patch:
    0.4.2 -> 0.4.3. Asserting the literal is what stops the field from landing
    under a minor bump, which under this package's 0.x rule
    (`version.is_compatible`: same major.minor) would make every pre-upgrade
    producer's event incompatible and dead-letter a rolling deploy's in-flight
    traffic for a field nobody is required to send.
    """

    before = tuple(int(part) for part in "0.4.2".split("."))
    with_attachments = tuple(int(part) for part in "0.4.3".split("."))

    assert with_attachments[:2] == before[:2]
    assert with_attachments[2] == before[2] + 1


def test_a_patch_difference_is_compatible_in_both_directions() -> None:
    """The consequence of classing this as a patch, asserted as behavior.

    A rolling deploy runs both images at once, so compatibility has to hold in
    BOTH directions: an old producer's 0.4.2 event reaching this consumer, and
    this producer's 0.4.3 event reaching a consumer still on 0.4.2. A minor
    difference remains incompatible, which is what keeps this assertion from
    being a tautology about any two version strings.
    """

    assert is_compatible("0.4.2", "0.4.3") is True
    assert is_compatible("0.4.3", "0.4.2") is True
    assert is_compatible("0.3.9", "0.4.3") is False
    assert is_compatible("0.4.3", "0.3.9") is False


def test_targetless_turns_start_a_new_incompatible_protocol_line() -> None:
    assert PROTOCOL_VERSION == "0.5.2"
    assert is_compatible("0.4.5", PROTOCOL_VERSION) is False
    assert is_compatible(PROTOCOL_VERSION, "0.4.5") is False


def test_a_payload_written_before_attachments_existed_decodes_with_none() -> None:
    """The compatibility claim that makes the patch bump honest.

    A turn minted by the previous image carries no `attachments` key at all --
    exactly what a mid-deploy dispatcher produces -- and it must still decode,
    with the field reading as "this channel reported nothing" rather than
    raising or arriving as None. The empty list is what lets a consumer write
    `for ref in turn.attachments` with no guard, so the default has to be the
    empty collection and not None.

    `pop` rather than `del` deliberately: the pre-upgrade shape is "the key is
    absent", and building it that way keeps this test honest whether or not the
    regenerated golden happens to serialize an empty list.
    """

    pre_upgrade = json.loads(_GOLDEN.read_text())
    pre_upgrade.pop("attachments", None)
    assert "attachments" not in pre_upgrade

    turn = QueuedTurn.model_validate(pre_upgrade, context={_READER_CONTEXT_KEY: True})

    assert turn.attachments == []

    # And the same absence through the sanctioned consumer decode, since that is
    # the path the worker actually takes off the Stream.
    assert parse_queued_turn(json.dumps(pre_upgrade)).attachments == []


def test_a_turn_built_with_no_attachments_round_trips_as_empty() -> None:
    """A first-party producer that carries no attachments is unchanged.

    Three of the four mint sites (api/resumequeue.py, api/routers/channels.py,
    api/routers/hooks.py) never see a file, so the default has to survive
    construction AND the wire without the producer naming it -- otherwise the
    "optional" classification is only true at the schema and not on the queue.
    """

    turn = QueuedTurn(
        event_id="e1",
        conversation_id="c1",
        author="u1",
        text="hi",
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="1.0"),
        received_at="2026-01-01T00:00:00Z",
    )

    assert turn.attachments == []
    assert parse_queued_turn(turn.model_dump_json()) == turn


def test_a_noncron_turn_can_omit_hook_run() -> None:
    turn = QueuedTurn(
        event_id="e1",
        conversation_id="c1",
        author="u1",
        text="hi",
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="1.0"),
        received_at="20260101T000000Z",
        source=TurnSource.SLACK,
    )

    assert turn.hook_run is None


def test_a_complete_hook_run_round_trips_through_the_queue_wire() -> None:
    hook_run = {
        "agent_id": "00000000000040008000000000000001",
        "name": "acme_nightly",
        "slot_utc": "20260922T030000Z",
    }
    turn = QueuedTurn(
        event_id="e1",
        conversation_id="c1",
        author="u1",
        text="hi",
        reply_handle=ReplyHandle(kind="slack", channel="C1", placeholder="1.0"),
        received_at="20260922T030000Z",
        source=TurnSource.CRON,
        hook_run=hook_run,
    )

    encoded = turn.model_dump_json()
    assert json.loads(encoded)["hook_run"] == hook_run

    restored = parse_queued_turn(encoded)
    assert restored == turn
    assert restored.hook_run is not None
    assert restored.hook_run.agent_id == hook_run["agent_id"]
    assert restored.hook_run.name == hook_run["name"]
    assert restored.hook_run.slot_utc == hook_run["slot_utc"]


@pytest.mark.parametrize("missing_field", ["agent_id", "name", "slot_utc"])
def test_a_partial_hook_run_is_rejected(missing_field: str) -> None:
    hook_run = {
        "agent_id": "00000000000040008000000000000001",
        "name": "acme_nightly",
        "slot_utc": "20260922T030000Z",
    }
    del hook_run[missing_field]

    with pytest.raises(ValidationError) as exc_info:
        QueuedTurn(
            event_id="e1",
            conversation_id="c1",
            author="u1",
            text="hi",
            reply_handle=ReplyHandle(
                kind="slack", channel="C1", placeholder="1.0"
            ),
            received_at="20260922T030000Z",
            source=TurnSource.CRON,
            hook_run=hook_run,
        )

    locations = {tuple(error["loc"]) for error in exc_info.value.errors()}
    assert ("hook_run", missing_field) in locations


def _targetless_cron_payload() -> dict[str, object]:
    return {
        "event_id": "e1",
        "conversation_id": "c1",
        "author": "cron",
        "text": "run the nightly report",
        "received_at": "20260922T030000Z",
        "source": "cron",
        "hook_run": {
            "agent_id": "00000000000040008000000000000001",
            "name": "acme_nightly",
            "slot_utc": "20260922T030000Z",
        },
    }


def _construct_or_parse_targetless(
    payload: dict[str, object], reader: str
) -> QueuedTurn:
    if reader == "constructor":
        return QueuedTurn(**payload)  # type: ignore[arg-type]
    return parse_queued_turn(json.dumps(payload))


@pytest.mark.parametrize("reader", ["constructor", "consumer"])
@pytest.mark.parametrize("explicit_null", [False, True], ids=["omitted", "null"])
def test_a_targetless_cron_turn_round_trips_with_complete_identity(
    explicit_null: bool,
    reader: str,
) -> None:
    payload = _targetless_cron_payload()
    if explicit_null:
        payload["reply_handle"] = None

    turn = _construct_or_parse_targetless(payload, reader)

    assert turn.reply_handle is None
    assert turn.hook_run == HookRunRef(
        agent_id="00000000000040008000000000000001",
        name="acme_nightly",
        slot_utc="20260922T030000Z",
    )
    assert parse_queued_turn(turn.model_dump_json()) == turn


@pytest.mark.parametrize("reader", ["constructor", "consumer"])
def test_a_targetless_cron_turn_without_run_identity_is_rejected(
    reader: str,
) -> None:
    payload = _targetless_cron_payload()
    payload["hook_run"] = None

    with pytest.raises(ValidationError):
        _construct_or_parse_targetless(payload, reader)


@pytest.mark.parametrize("reader", ["constructor", "consumer"])
def test_a_targetless_cron_turn_with_omitted_run_identity_is_rejected(
    reader: str,
) -> None:
    payload = _targetless_cron_payload()
    del payload["hook_run"]

    with pytest.raises(ValidationError):
        _construct_or_parse_targetless(payload, reader)


@pytest.mark.parametrize("reader", ["constructor", "consumer"])
@pytest.mark.parametrize("missing_field", ["agent_id", "name", "slot_utc"])
def test_a_targetless_cron_turn_with_partial_identity_is_rejected(
    missing_field: str,
    reader: str,
) -> None:
    payload = _targetless_cron_payload()
    hook_run = dict(payload["hook_run"])  # type: ignore[arg-type]
    del hook_run[missing_field]
    payload["hook_run"] = hook_run

    with pytest.raises(ValidationError):
        _construct_or_parse_targetless(payload, reader)


@pytest.mark.parametrize("reader", ["constructor", "consumer"])
@pytest.mark.parametrize("field", ["agent_id", "name", "slot_utc"])
@pytest.mark.parametrize(
    "invalid_value",
    [
        pytest.param(None, id="null"),
        pytest.param("", id="empty"),
        pytest.param(" \t", id="whitespace"),
        pytest.param(7, id="nonstring"),
    ],
)
def test_a_targetless_cron_turn_with_invalid_identity_is_rejected(
    field: str,
    invalid_value: object,
    reader: str,
) -> None:
    payload = _targetless_cron_payload()
    hook_run = dict(payload["hook_run"])  # type: ignore[arg-type]
    hook_run[field] = invalid_value
    payload["hook_run"] = hook_run

    with pytest.raises(ValidationError):
        _construct_or_parse_targetless(payload, reader)


@pytest.mark.parametrize("reader", ["constructor", "consumer"])
@pytest.mark.parametrize(
    "source",
    [
        pytest.param(None, id="default_slack"),
        pytest.param(TurnSource.SLACK.value, id="slack"),
        pytest.param(TurnSource.WEBHOOK.value, id="webhook"),
    ],
)
def test_only_cron_may_be_targetless(source: str | None, reader: str) -> None:
    payload = _targetless_cron_payload()
    if source is None:
        del payload["source"]
    else:
        payload["source"] = source
    payload["reply_handle"] = None

    with pytest.raises(ValidationError):
        _construct_or_parse_targetless(payload, reader)


@pytest.mark.parametrize(
    "source",
    [TurnSource.SLACK, TurnSource.WEBHOOK, TurnSource.CRON],
)
def test_a_targeted_turn_still_round_trips_for_every_source(
    source: TurnSource,
) -> None:
    hook_run = (
        HookRunRef(
            agent_id="00000000000040008000000000000001",
            name="acme_nightly",
            slot_utc="20260922T030000Z",
        )
        if source is TurnSource.CRON
        else None
    )
    turn = QueuedTurn(
        event_id="e1",
        conversation_id="c1",
        author="u1",
        text="hi",
        reply_handle=ReplyHandle(
            kind="slack",
            channel="C1",
            placeholder="1.0",
        ),
        received_at="20260922T030000Z",
        source=source,
        hook_run=hook_run,
    )

    restored = parse_queued_turn(turn.model_dump_json())

    assert restored == turn
    assert restored.reply_handle == turn.reply_handle
    assert restored.source is source


def test_a_slack_route_without_an_adapter_is_the_default_identity() -> None:
    # A handle queued before ADR-0168 decision 3, or an approval row decision 5
    # has not backfilled yet, still carries NULL. It means the one Slack app.
    assert route_identity(SLACK_KIND, None) == DEFAULT_IDENTITY == "default"


def test_a_named_slack_identity_is_kept() -> None:
    assert route_identity("slack", "support-bot") == "support-bot"


def test_another_kind_keeps_its_adapter_or_its_absence() -> None:
    assert route_identity("email", "agentmail-sandbox") == "agentmail-sandbox"
    # A route-less non-Slack binding stays route-less: NULL is not an identity.
    assert route_identity("email", None) is None


@dataclass(frozen=True)
class _Row:
    """A minimal stand-in for any row `matching_routes` can read: an ORM
    object, a SQLAlchemy `Row`, or a plain object -- the function only ever
    touches these four attributes."""

    kind: str
    address: str
    adapter: str | None
    endpoint: str | None


def test_a_slack_turn_with_no_adapter_matches_the_stored_default_row() -> None:
    default_row = _Row(kind="slack", address="C0EXAMPLE1", adapter=None, endpoint=None)
    other_pair = _Row(kind="slack", address="C0EXAMPLE2", adapter=None, endpoint=None)

    assert matching_routes([default_row, other_pair], "slack", "C0EXAMPLE1", None) == [
        default_row
    ]


def test_a_slack_turn_with_adapter_default_matches_the_same_null_row() -> None:
    default_row = _Row(kind="slack", address="C0EXAMPLE1", adapter=None, endpoint=None)

    assert matching_routes([default_row], "slack", "C0EXAMPLE1", "default") == [default_row]


def test_a_slack_turn_with_a_named_adapter_does_not_match_the_default_row() -> None:
    default_row = _Row(kind="slack", address="C0EXAMPLE1", adapter=None, endpoint=None)

    assert matching_routes([default_row], "slack", "C0EXAMPLE1", "second") == []


def test_a_slack_turn_with_no_adapter_falls_back_to_the_custom_transport_row() -> None:
    # The pre-ADR custom-transport form (e.g. the offline hook-approval proof
    # rig) stores a credential slug in `adapter`, not an identity, so it never
    # matches `DEFAULT_IDENTITY` directly -- the omitted-adapter selector must
    # still fall back to the one row on the pair that carries an endpoint.
    custom_transport = _Row(
        kind="slack", address="C0EXAMPLE1", adapter="proof-offline", endpoint="http://127.0.0.1:1"
    )

    assert matching_routes([custom_transport], "slack", "C0EXAMPLE1", None) == [custom_transport]


# `curie cluster message` relays a turn with the worker's built-in reply
# adapter. That adapter picks where the reply is delivered, not which binding
# answers: the turn is still the channel's own Slack turn.
_CLUSTER_MESSAGE_ADAPTER = "curie-cluster-message"


def test_the_cluster_message_relay_adapter_is_not_an_identity() -> None:
    assert route_identity(SLACK_KIND, _CLUSTER_MESSAGE_ADAPTER) == DEFAULT_IDENTITY


def test_a_cluster_message_relay_turn_matches_the_default_row() -> None:
    default_row = _Row(kind="slack", address="C0EXAMPLE1", adapter=None, endpoint=None)
    named_row = _Row(kind="slack", address="C0EXAMPLE1", adapter="second", endpoint=None)

    assert matching_routes(
        [default_row, named_row], "slack", "C0EXAMPLE1", _CLUSTER_MESSAGE_ADAPTER
    ) == [default_row]


def test_a_cluster_message_relay_turn_falls_back_to_the_custom_transport_row() -> None:
    # Before the route triple a relay turn resolved on (kind, address) alone,
    # so it reached this row the same as a turn with no adapter does.
    custom_transport = _Row(
        kind="slack", address="C0EXAMPLE1", adapter="proof-offline", endpoint="http://127.0.0.1:1"
    )

    assert matching_routes(
        [custom_transport], "slack", "C0EXAMPLE1", _CLUSTER_MESSAGE_ADAPTER
    ) == [custom_transport]


def test_the_custom_transport_fallback_does_not_fire_with_a_given_adapter() -> None:
    # The fallback is reserved for an OMITTED adapter (the old callers of a
    # custom-transport row never send one). A turn that names an adapter and
    # misses gets no match, never the endpoint-carrying row instead.
    custom_transport = _Row(
        kind="slack", address="C0EXAMPLE1", adapter="proof-offline", endpoint="http://127.0.0.1:1"
    )

    assert matching_routes([custom_transport], "slack", "C0EXAMPLE1", "second") == []


def test_the_custom_transport_fallback_does_not_fire_when_a_default_row_matches() -> None:
    # The fallback only runs when the identity comparison found NOTHING. A
    # stored default row always wins that comparison outright, so the
    # fallback must not also pull in an unrelated endpoint-carrying row on
    # the same pair (which the unique `(kind, address)` constraint rules out
    # until the contract migration for ADR-0168 decision 3 (#3100) widens it,
    # but the function's own logic must not assume that).
    default_row = _Row(kind="slack", address="C0EXAMPLE1", adapter=None, endpoint=None)
    custom_transport = _Row(
        kind="slack", address="C0EXAMPLE1", adapter="proof-offline", endpoint="http://127.0.0.1:1"
    )

    assert matching_routes([default_row, custom_transport], "slack", "C0EXAMPLE1", None) == [
        default_row
    ]


def test_a_non_slack_turn_with_an_adapter_matches_only_its_own_row() -> None:
    named = _Row(
        kind="webhook", address="https://example.test/hook", adapter="acme", endpoint="http://a/"
    )
    other = _Row(
        kind="webhook", address="https://example.test/hook", adapter="other", endpoint="http://b/"
    )

    assert matching_routes([named, other], "webhook", "https://example.test/hook", "acme") == [
        named
    ]
    assert matching_routes([named, other], "webhook", "https://example.test/hook", "missing") == []


def test_a_non_slack_turn_with_no_adapter_matches_every_row_on_the_pair() -> None:
    # Migration 0023's pair constraint holds a non-Slack pair to one row, so
    # this is only reachable with rows written out of band, but the omitted
    # selector's semantics are still "every row on the pair", not "none".
    first = _Row(
        kind="webhook", address="https://example.test/hook", adapter="acme", endpoint="http://a/"
    )
    second = _Row(
        kind="webhook", address="https://example.test/hook", adapter="other", endpoint="http://b/"
    )

    assert matching_routes([first, second], "webhook", "https://example.test/hook", None) == [
        first,
        second,
    ]
