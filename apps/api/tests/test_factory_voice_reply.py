"""Voice replies are bound to the version printed in the source conversation."""

from datetime import UTC, datetime
from uuid import UUID

import pytest
from curie_api.factory_voice_contract import (
    Draft,
    Edit,
    GitHubConversation,
    Refused,
    VoiceReply,
    parse_voice_reply,
)


def test_displayed_version_routes_the_decision_and_preserves_an_edit() -> None:
    draft = Draft(
        draft_id=UUID("abcd1234-ef01-4a66-8080-123456789abc"),
        audience=GitHubConversation(
            host="github.com", installation_id=1, repository_id=2, kind="issue", number=3
        ),
        transcript="change the label",
        version=2,
        expires_at=datetime(2026, 1, 1, tzinfo=UTC),
    )
    assert draft.code == "abcd1234ef01.2"
    assert parse_voice_reply(f"confirm {draft.code}") == VoiceReply(
        code=draft.code, action="confirm"
    )
    assert parse_voice_reply(f"reject {draft.code}") == VoiceReply(code=draft.code, action="reject")
    edit = " Keep the timeout.\nDo not change it."
    assert parse_voice_reply(f"edit {draft.code} {edit}") == VoiceReply(
        code=draft.code, action=Edit(text=edit)
    )


@pytest.mark.parametrize("text", ["confirm", "reject the patch", "edit the timeout", "confirm 23"])
def test_ordinary_feedback_does_not_route_to_a_draft(text: str) -> None:
    assert parse_voice_reply(text) is None


def test_extra_text_after_a_coded_confirm_refuses_the_decision() -> None:
    assert parse_voice_reply("confirm abcd1234ef01.2 extra") == Refused(reason="invalid_text")
