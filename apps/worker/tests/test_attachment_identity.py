"""An attachment is fetched with the bot token of the identity it arrived on.

ADR-0168 decision 5: `files:read` is granted per Slack app, so a file posted
to a named bot is readable only with that bot's token.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest
from aci_protocol import Attachment

# importlib import mode does not add this test directory to sys.path.
sys.path.insert(0, str(Path(__file__).parent))

import curie_worker.attachments as attachments_module  # noqa: E402
from attachment_fixtures import (  # noqa: E402
    AGENT_ID,
    THREAD_KEY,
    FakeSlackFiles,
    MovableClock,
    RetainingObjectStore,
    limits,
)
from curie_worker.attachments import (  # noqa: E402
    AttachmentCoordinator,
    AttachmentResolutionError,
)

_REF = Attachment(id="F1", name="report.csv", mime_type="text/csv")


def _lane(
    default: FakeSlackFiles, named: dict[str, FakeSlackFiles]
) -> tuple[AttachmentCoordinator, RetainingObjectStore]:
    store = RetainingObjectStore()
    lane = AttachmentCoordinator(
        files=default,
        identity_files=named,
        objects=store,
        limits=limits(attachments_module),
        clock=MovableClock(),
    )
    return lane, store


def _resolve(lane: AttachmentCoordinator, **identity: str) -> object:
    return lane.resolve(
        thread_key=THREAD_KEY,
        agent_id=AGENT_ID,
        attachments=[_REF],
        generation="gen-1",
        **identity,
    )


def test_a_named_identitys_file_is_fetched_through_its_own_client() -> None:
    default = FakeSlackFiles({"F1": [b"default"]})
    ops = FakeSlackFiles({"F1": [b"ops"]})
    lane, _store = _lane(default, {"ops-bot": ops})

    _resolve(lane, identity="ops-bot")

    assert ops.requested == ["F1"]
    assert default.requested == []


@pytest.mark.parametrize("identity", [{}, {"identity": "default"}])
def test_default_and_an_unnamed_turn_keep_the_default_client(identity: dict[str, str]) -> None:
    default = FakeSlackFiles({"F1": [b"default"]})
    ops = FakeSlackFiles({"F1": [b"ops"]})
    lane, _store = _lane(default, {"ops-bot": ops})

    _resolve(lane, **identity)

    assert default.requested == ["F1"]
    assert ops.requested == []


def test_an_identity_with_no_client_is_refused_before_any_fetch() -> None:
    default = FakeSlackFiles({"F1": [b"default"]})
    lane, store = _lane(default, {})

    with pytest.raises(AttachmentResolutionError, match="'ghost'") as refused:
        _resolve(lane, identity="ghost")

    assert refused.value.stage == "credential"
    assert default.requested == []
    assert store.objects == {}
