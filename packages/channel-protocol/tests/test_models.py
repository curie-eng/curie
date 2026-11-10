import pytest
from channel_protocol import (
    Action,
    ChannelCapabilities,
    ChannelCapability,
    ChoiceIntent,
    ConfirmIntent,
    OutboundMessage,
)
from pydantic import ValidationError


def test_choice_message_round_trips() -> None:
    message = OutboundMessage(
        version="1.0",
        text="Pick a repository.",
        interaction=ChoiceIntent(
            kind="choice",
            id="repo",
            options=[Action(label="Curie", value="curie-eng/curie")],
        ),
    )
    decoded = OutboundMessage.model_validate_json(message.model_dump_json())
    assert isinstance(decoded.interaction, ChoiceIntent)
    assert decoded.interaction.options[0].value == "curie-eng/curie"


def test_confirm_is_semantic_and_free_text_is_off_by_default() -> None:
    message = OutboundMessage(
        version="1.0",
        text="Deploy this change?",
        interaction=ConfirmIntent(
            kind="confirm",
            id="deploy",
            prompt="Deploy this change?",
            confirm=Action(label="Deploy", value="deploy"),
            cancel=Action(label="Cancel", value="cancel"),
        ),
    )
    assert isinstance(message.interaction, ConfirmIntent)
    assert message.interaction.allow_free_text is False


def test_text_fallback_is_required_and_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        OutboundMessage.model_validate({"version": "1.0"})
    with pytest.raises(ValidationError):
        OutboundMessage.model_validate({"version": "1.0", "text": "ok", "blocks": []})


def test_history_read_capability_round_trips() -> None:
    # ADR 0100 section 2: an adapter that can retain history advertises the
    # read half; one that cannot simply leaves it out of its list.
    advertised = ChannelCapabilities(
        version="1.0", capabilities=[ChannelCapability("history-read")]
    )
    decoded = ChannelCapabilities.model_validate_json(advertised.model_dump_json())
    assert decoded.capabilities == [ChannelCapability.HISTORY_READ]
    assert ChannelCapability.HISTORY_READ.value == "history-read"
    assert ChannelCapabilities.model_validate_json(
        '{"version": "1.0", "capabilities": ["history-read", "threading"]}'
    ).capabilities == [ChannelCapability.HISTORY_READ, ChannelCapability.THREADING]


@pytest.mark.parametrize(
    ("wire", "member"),
    [
        ("canvas-read", ChannelCapability.CANVAS_READ),
        ("canvas-edit", ChannelCapability.CANVAS_EDIT),
    ],
)
def test_canvas_capabilities_round_trip(wire: str, member: ChannelCapability) -> None:
    # ADR 0200: an adapter that can list and read canvases advertises
    # canvas-read, one that can replace a cell advertises canvas-edit, and an
    # adapter without canvases advertises neither.
    advertised = ChannelCapabilities(version="1.0", capabilities=[ChannelCapability(wire)])
    decoded = ChannelCapabilities.model_validate_json(advertised.model_dump_json())
    assert decoded.capabilities == [member]
    assert member.value == wire
    assert ChannelCapabilities.model_validate_json(
        f'{{"version": "1.0", "capabilities": ["history-read", "{wire}"]}}'
    ).capabilities == [ChannelCapability.HISTORY_READ, member]
    assert ChannelCapabilities.model_validate_json(
        '{"version": "1.0", "capabilities": ["history-read"]}'
    ).capabilities == [ChannelCapability.HISTORY_READ]
