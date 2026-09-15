"""Attachment refs are minted BESIDE the text, not instead of it (#2567).

The defect this file pins: a Slack message with a non-empty `text` AND a `files`
array reached the worker with the text intact and the files gone. `derive_text`
never sees those files -- by design, because a non-empty top-level `text` is
returned byte-identically and "nothing else is consulted"
(`inbound_text.derive_text`'s docstring). That passthrough is a deliberate
decision from #2006 and this ticket must not reverse it, so the refs ride a
parallel `QueuedTurn.attachments` field populated at the mint site
(`handlers.py::_mint_turn`) independently of the text derivation.

Every test below therefore asserts BOTH halves of that independence at once:
the text is byte-identical to what Slack sent, and the refs are present. A test
that only checked the refs could pass against an implementation that started
folding filenames into the prompt, which is the change #2006 argued against.

The mint site is driven through `process_event` against the REAL Valkey from the
compose stack (repo test discipline: never mock Valkey) and read back with
`from_stream_fields`, the same round trip the worker performs -- so a ref that
survives construction but not the Stream encoding fails here.

Slack payload shapes are cited to Slack's own reference plus the file entries
already observed in this repo's suites; never to what the implementation assumes.
"""

from typing import Any

import pytest
import redis
from aci_protocol import QueuedTurn
from curie_dispatcher.config import DispatcherConfig
from curie_dispatcher.handlers import process_event
from curie_dispatcher.inbound_text import derive_text
from curie_dispatcher.queue import from_stream_fields

PLACEHOLDER_TS = "1700000000.000200"

#: The comment a person types when they upload a file. Non-empty on purpose:
#: this is the half of the payload that already worked, and it must stay
#: byte-identical -- surrounding whitespace and all -- while the refs are added.
COMMENT = "  here is the incident report  "


class _WebClient:
    """Minimal Slack Web API fake: the placeholder post is all the mint site calls."""

    def chat_postMessage(self, **_kwargs: Any) -> dict[str, str]:
        return {"ts": PLACEHOLDER_TS}


def _file_share_event(
    *,
    text: str = COMMENT,
    files: Any = None,
    include_files: bool = True,
) -> dict[str, Any]:
    """A `message.file_share` delivery on the DM lane.

    Slack: https://docs.slack.dev/reference/events/message.file_share -- a
    person uploading a file with a comment. The same shape is already exercised
    in this repo as an observed payload: `apps/dispatcher/tests/
    test_inbound_relevance.py` (the `dm_file_share_with_comment` row, ~line 490)
    delivers `text="here is the incident report"` along
    `files=[{"id": ..., "title": ..., "name": ...}]`, and
    `apps/dispatcher/tests/test_inbound_text.py` (~line 653) uses the same
    `id`/`title`/`name` file-entry keys. `channel_type: "im"` is what Slack
    stamps on the DM lane the app manifest subscribes to.
    """
    event: dict[str, Any] = {
        "type": "message",
        "subtype": "file_share",
        "channel_type": "im",
        "channel": "D1",
        "user": "U9",
        "text": text,
        "ts": "1800.0001",
    }
    if include_files:
        event["files"] = files
    return event


def _mint(
    event: dict[str, Any],
    *,
    redis_client: redis.Redis,
    config: DispatcherConfig,
    event_id: str,
) -> QueuedTurn:
    """Run the real mint site and read the turn back off the real Stream."""
    stream_id = process_event(
        body={"event_id": event_id},
        event=event,
        lane="im",
        web_client=_WebClient(),  # type: ignore[arg-type]
        redis_client=redis_client,
        config=config,
    )
    assert stream_id is not None, "the delivery must be enqueued, not refused"
    entries = redis_client.xrange(config.stream)
    assert len(entries) == 1
    return from_stream_fields(entries[0][1])


# ---------------------------------------------------------------------------
# The headline regression: text AND files, both carried
# ---------------------------------------------------------------------------


def test_a_comment_with_files_carries_the_refs_and_the_untouched_text(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """THE regression. Before #2567 the refs were dropped on exactly this shape.

    Slack file objects: https://docs.slack.dev/reference/objects/file-object --
    `id` is the file's opaque identifier and `name` its filename. Both are
    observed in this repo's existing fixtures (see `_file_share_event`).

    `derive_text` is asserted here too, unchanged: it returns the comment
    byte-identically and contributes nothing from `files`, which is precisely
    why the refs need their own field.
    """

    event = _file_share_event(
        files=[
            {"id": "F0EXAMPLE1", "title": "incident.pdf", "name": "incident.pdf"},
            {"id": "F0EXAMPLE2", "name": "screenshot.png"},
        ]
    )

    # The prior decision, restated as a precondition rather than reversed.
    assert derive_text(event) == COMMENT

    turn = _mint(event, redis_client=redis_client, config=config, event_id="Ev-attach-1")

    assert turn.text == COMMENT, "the comment must survive byte-identically"
    assert [(ref.id, ref.name) for ref in turn.attachments] == [
        ("F0EXAMPLE1", "incident.pdf"),
        ("F0EXAMPLE2", "screenshot.png"),
    ]


def test_a_bare_file_share_carries_refs_while_the_text_still_derives(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The other direction of the same independence.

    A file dropped with no comment still derives its filename as the prompt
    (#2006's `files` branch, `test_inbound_text.py::
    test_files_contribute_title_or_name_per_file`). Adding refs must not
    displace that derivation -- the turn needs both, and this asserts the new
    field did not become the derivation's replacement.
    """

    event = _file_share_event(
        text="",
        files=[{"id": "F0EXAMPLE1", "title": "incident.pdf", "name": "incident.pdf"}],
    )

    turn = _mint(event, redis_client=redis_client, config=config, event_id="Ev-attach-2")

    assert turn.text == "incident.pdf"
    assert [ref.id for ref in turn.attachments] == ["F0EXAMPLE1"]


def test_a_message_with_no_files_carries_no_refs(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The falsifiable control: refs appear because files were present.

    Without this, an implementation that stamped a constant list would pass
    every positive test above.
    """

    event = _file_share_event(include_files=False)
    del event["subtype"]

    turn = _mint(event, redis_client=redis_client, config=config, event_id="Ev-attach-3")

    assert turn.text == COMMENT
    assert turn.attachments == []


# ---------------------------------------------------------------------------
# Optional metadata: Slack's key names are `mimetype` and `size`
# ---------------------------------------------------------------------------


def test_mime_type_and_size_populate_from_slacks_own_key_names(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """The wire field names are ours; the SOURCE key names are Slack's.

    Slack file object: https://docs.slack.dev/reference/objects/file-object --
    the keys are `mimetype` (one word, no underscore) and `size` (bytes), NOT
    `mime_type` and `size_bytes`. Getting this wrong is silent: the refs still
    arrive, just with both descriptive fields None, and nothing fails. The
    file entries observed in this repo (`test_inbound_relevance.py` ~line 490
    and `test_inbound_text.py` ~line 653) carry only `id`/`title`/`name`, so
    they ground the identity keys but not these two; the mapping asserted here
    rests on the provider reference above.
    """

    event = _file_share_event(
        files=[
            {
                "id": "F0EXAMPLE1",
                "name": "incident.pdf",
                "title": "incident.pdf",
                "mimetype": "application/pdf",
                "size": 12345,
            },
            {"id": "F0EXAMPLE2", "name": "screenshot.png"},
        ]
    )

    turn = _mint(event, redis_client=redis_client, config=config, event_id="Ev-attach-4")

    assert turn.attachments[0].mime_type == "application/pdf"
    assert turn.attachments[0].size_bytes == 12345
    # A channel that reports neither yields None rather than a fabricated value.
    assert turn.attachments[1].mime_type is None
    assert turn.attachments[1].size_bytes is None


# ---------------------------------------------------------------------------
# Malformed `files`: never raise, yield nothing
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("files", "include_files"),
    [
        pytest.param(None, False, id="files-key-absent"),
        pytest.param(None, True, id="files-none"),
        pytest.param("incident.pdf", True, id="files-not-a-list"),
        pytest.param({"id": "F1", "name": "x.pdf"}, True, id="files-a-bare-dict"),
        pytest.param([], True, id="files-empty-list"),
        pytest.param(["incident.pdf"], True, id="entry-is-a-bare-string"),
        pytest.param([None], True, id="entry-is-none"),
        pytest.param([{}], True, id="entry-is-an-empty-dict"),
        pytest.param([{"name": "incident.pdf"}], True, id="entry-missing-id"),
        pytest.param([{"id": "F0EXAMPLE1"}], True, id="entry-missing-name"),
        pytest.param([{"id": None, "name": None}], True, id="entry-with-null-fields"),
        pytest.param([{"id": 42, "name": 7}], True, id="entry-with-non-string-fields"),
    ],
)
def test_malformed_files_never_raise_and_yield_no_refs(
    files: Any,
    include_files: bool,
    redis_client: redis.Redis,
    config: DispatcherConfig,
) -> None:
    """The never-raise contract, borrowed deliberately from `derive_text`.

    `derive_text`'s docstring promises it "never raises on a malformed payload".
    The mint site runs AFTER the idempotency claim and AFTER the placeholder is
    already visible in the thread, so a raise here is strictly worse than a
    dropped ref: Bolt has acked, the claim survives, the placeholder sits in the
    channel forever, and Slack's redelivery is refused as an already-seen
    delivery (`queue.py::release_event`'s asymmetry, #2006). A ref that cannot
    be built is therefore skipped, and the turn is minted anyway.

    An entry missing `id` or missing `name` is skipped rather than defaulted:
    `Attachment` requires both, and inventing either would produce a ref that
    reads as resolvable and is not.
    """

    event = _file_share_event(files=files, include_files=include_files)

    turn = _mint(event, redis_client=redis_client, config=config, event_id="Ev-attach-bad")

    # The turn still exists, with its text untouched -- the loss is bounded to
    # the ref that could not be built.
    assert turn.text == COMMENT
    assert turn.attachments == []


def test_one_malformed_entry_does_not_discard_its_well_formed_neighbours(
    redis_client: redis.Redis, config: DispatcherConfig
) -> None:
    """Bounded loss, stated precisely: the bad entry, not the whole array.

    Slack ships new file fields and shapes on its own schedule, so an entry this
    adapter cannot read must cost exactly that entry. Discarding the array (or
    raising) would turn one unexpected file into a total loss of every ref on a
    multi-file upload.
    """

    event = _file_share_event(
        files=[
            {"id": "F0EXAMPLE1", "name": "incident.pdf"},
            None,
            {"name": "no-id.png"},
            {"id": "F0EXAMPLE2", "name": "screenshot.png"},
        ]
    )

    turn = _mint(event, redis_client=redis_client, config=config, event_id="Ev-attach-mixed")

    assert turn.text == COMMENT
    assert [ref.id for ref in turn.attachments] == ["F0EXAMPLE1", "F0EXAMPLE2"]
