"""Pure unit tests for the Slack inbound-text walker (`inbound_text.derive_text`, D1).

No Bolt, no Valkey, no network -- ``inbound_text`` is stdlib-only and these
tests exercise it directly with plain dict fixtures. The end-to-end Block Kit
regression driven through Bolt lives in ``test_inbound_relevance.py`` (Stream
B); this file is the walker's own behavior table: byte-identical top-level
passthrough, block/attachment/file derivation, the UI-chrome key denylist,
the three traversal bounds, and every degenerate input D1 specifies.

Every assertion about a Slack event, block, or attachment shape is grounded in
a comment citing Slack's own reference docs -- never in an assumption about
what the walker will do.
"""

import time
from typing import Any

import pytest
from curie_channel_gateway.slack.inbound_text import _MAX_CHARS, _MAX_DEPTH, _MAX_NODES, derive_text


def _wrap_in_depth(inner: Any, levels: int) -> Any:
    """Nest ``inner`` inside ``levels`` single-item lists.

    A list is one of the two generic recursion containers D1 names ("recurse
    into dict values and list items"), so this is a container-agnostic way to
    push a leaf node past ``_MAX_DEPTH`` without depending on which block type
    the implementation happens to nest through.
    """
    node = inner
    for _ in range(levels):
        node = [node]
    return node


# ---------------------------------------------------------------------------
# Top-level text: byte-identical passthrough, whitespace-only falls through
# ---------------------------------------------------------------------------


def test_nonempty_top_level_text_is_returned_byte_identical() -> None:
    # https://docs.slack.dev/reference/events/app_mention -- the event's
    # top-level ``text`` carries the message body verbatim, including
    # ``<@U…>`` mention markup the worker relies on for addressing, and any
    # leading/trailing whitespace the user actually sent.
    event: dict[str, Any] = {
        "type": "app_mention",
        "text": "  <@U123> please look at this  ",
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": "SHOULD_NOT_APPEAR"}},
        ],
        "attachments": [{"text": "SHOULD_NOT_APPEAR_EITHER"}],
    }
    assert derive_text(event) == "  <@U123> please look at this  "


def test_whitespace_only_top_level_text_falls_through_to_blocks() -> None:
    # https://docs.slack.dev/reference/events/app_mention -- an alert-style
    # app_mention with an all-whitespace ``text`` and a Block Kit body: the
    # exact "emptied, not dropped" shape this ticket fixes (D1).
    event: dict[str, Any] = {
        "type": "app_mention",
        "text": "   ",
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": "Derived from blocks"}},
        ],
    }
    assert derive_text(event) == "Derived from blocks"


# ---------------------------------------------------------------------------
# The alert-shaped Block Kit body (AC 1's shape)
# ---------------------------------------------------------------------------


def test_alert_shaped_blocks_payload_derives_every_element_in_order() -> None:
    """A realistic app_mention alert body: header, section text, section
    fields, a rich_text block with nested rich_text_section elements, and a
    context block. Every block shape below matches Slack's own reference.
    """
    event: dict[str, Any] = {
        "type": "app_mention",
        "text": "",
        "blocks": [
            # https://docs.slack.dev/reference/block-kit/blocks/header-block/
            {
                "type": "header",
                "block_id": "hdr1",
                "text": {"type": "plain_text", "text": "Deploy Alert"},
            },
            # https://docs.slack.dev/reference/block-kit/blocks/section-block/
            {
                "type": "section",
                "block_id": "sec1",
                "text": {"type": "mrkdwn", "text": "*Service* is unhealthy"},
            },
            {
                "type": "section",
                "block_id": "sec2",
                "fields": [
                    {"type": "mrkdwn", "text": "*Region:*\nus-east-1"},
                    {"type": "mrkdwn", "text": "*Status:*\nCritical"},
                ],
            },
            # https://docs.slack.dev/reference/block-kit/blocks/rich-text-block/
            {
                "type": "rich_text",
                "block_id": "rt1",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {"type": "text", "text": "See "},
                            {
                                "type": "link",
                                "url": "https://example.com/runbook",
                                "text": "the runbook",
                            },
                            {"type": "link", "url": "https://example.com/bare"},
                            {"type": "emoji", "name": "fire"},
                            {"type": "user", "user_id": "U456"},
                            {"type": "broadcast", "range": "here"},
                        ],
                    }
                ],
            },
            # https://docs.slack.dev/reference/block-kit/blocks/context-block/
            {
                "type": "context",
                "block_id": "ctx1",
                "elements": [
                    {"type": "mrkdwn", "text": "Reported by the monitor"},
                ],
            },
        ],
    }

    derived = derive_text(event)

    for expected in (
        "Deploy Alert",
        "Service* is unhealthy",
        "Region:",
        "us-east-1",
        "Status:",
        "See ",
        "the runbook",
        "https://example.com/bare",
        ":fire:",
        "<@U456>",
        "<!here>",
        "Reported by the monitor",
    ):
        assert expected in derived, f"missing {expected!r} in {derived!r}"

    # Slack's own serialization order: header, section text, section fields,
    # the rich_text run, then the context note (D1: "adequate", not a claim
    # of Slack "document order").
    positions = [
        derived.index(s)
        for s in (
            "Deploy Alert",
            "Service* is unhealthy",
            "Region:",
            "See ",
            "the runbook",
            "https://example.com/bare",
            ":fire:",
            "<@U456>",
            "<!here>",
            "Reported by the monitor",
        )
    ]
    assert positions == sorted(positions)


def test_rich_text_element_types_render_per_type() -> None:
    # https://docs.slack.dev/reference/block-kit/blocks/rich-text-block/
    # -- every documented rich_text_section element type this ticket's table
    # names, each asserted on its exact rendered form (D1).
    event: dict[str, Any] = {
        "blocks": [
            {
                "type": "rich_text",
                "elements": [
                    {
                        "type": "rich_text_section",
                        "elements": [
                            {"type": "text", "text": "plain run"},
                            {
                                "type": "link",
                                "url": "https://x.example/a",
                                "text": "labeled link",
                            },
                            {"type": "link", "url": "https://x.example/b"},
                            {"type": "emoji", "name": "tada"},
                            {"type": "user", "user_id": "U789"},
                            {"type": "channel", "channel_id": "C456"},
                            {"type": "usergroup", "usergroup_id": "S123"},
                            {"type": "broadcast", "range": "here"},
                        ],
                    }
                ],
            }
        ],
    }
    derived = derive_text(event)
    assert "plain run" in derived
    assert "labeled link" in derived
    assert "https://x.example/b" in derived
    assert ":tada:" in derived
    assert "<@U789>" in derived
    assert "<#C456>" in derived
    assert "<!subteam^S123>" in derived
    assert "<!here>" in derived


# ---------------------------------------------------------------------------
# Attachments: explicit-branch keys, nested blocks, fallback, double-counting
# ---------------------------------------------------------------------------


def test_attachments_only_payload_derives_pretext_title_text_fields_footer() -> None:
    # https://docs.slack.dev/messaging/legacy-secondary-message-attachments/
    # -- the message-attachment shape (pretext, title, text, fields, footer),
    # still delivered by legacy integrations and alerting bots.
    event: dict[str, Any] = {
        "type": "app_mention",
        "text": "",
        "attachments": [
            {
                "pretext": "Incoming alert",
                "title": "Disk usage critical",
                "text": "Volume /data is at 97% capacity.",
                "fields": [
                    {"title": "Host", "value": "db-7", "short": True},
                    {"title": "Threshold", "value": "90%", "short": True},
                ],
                "footer": "monitoring-bot",
            }
        ],
    }

    derived = derive_text(event)

    for expected in (
        "Incoming alert",
        "Disk usage critical",
        "Volume /data is at 97% capacity.",
        "Host",
        "db-7",
        "Threshold",
        "90%",
        "monitoring-bot",
    ):
        assert expected in derived


def test_attachment_nested_blocks_are_included_and_not_double_counted() -> None:
    """An attachment's own ``text``/``title`` are emitted by the explicit
    attachment branch; its nested ``blocks`` are walked too, but the
    attachment dict itself must not additionally be handed to the generic
    walker -- each piece appears exactly once (D1).
    """
    event: dict[str, Any] = {
        "attachments": [
            {
                "title": "Build failed",
                "text": "pytest exited non-zero",
                "blocks": [
                    {
                        "type": "section",
                        "text": {"type": "mrkdwn", "text": "See the CI log for details"},
                    }
                ],
            }
        ],
    }

    derived = derive_text(event)

    assert derived.count("Build failed") == 1
    assert derived.count("pytest exited non-zero") == 1
    assert derived.count("See the CI log for details") == 1


def test_attachment_with_real_content_does_not_emit_fallback() -> None:
    # D1: "fallback is used only when the attachment yielded nothing else."
    event: dict[str, Any] = {
        "attachments": [
            {
                "text": "Real content here",
                "fallback": "SHOULD_NOT_APPEAR_fallback_text",
            }
        ],
    }
    derived = derive_text(event)
    assert "Real content here" in derived
    assert "SHOULD_NOT_APPEAR_fallback_text" not in derived


def test_attachment_with_only_fallback_emits_it() -> None:
    event: dict[str, Any] = {
        "attachments": [{"fallback": "Plain text summary of this attachment"}],
    }
    assert derive_text(event) == "Plain text summary of this attachment"


def test_attachment_whose_only_content_is_chrome_still_emits_fallback() -> None:
    """The key denylist is what makes "fallback only when nothing else
    yielded" correct: chrome (a button label) must not count as "something
    else", or this attachment would silently lose its fallback (D1).
    """
    event: dict[str, Any] = {
        "attachments": [
            {
                "fallback": "Please upgrade your Slack client to see this",
                "blocks": [
                    {
                        "type": "actions",
                        "elements": [
                            {
                                "type": "button",
                                "text": {"type": "plain_text", "text": "Approve"},
                                "action_id": "approve1",
                            }
                        ],
                    }
                ],
            }
        ],
    }
    derived = derive_text(event)
    assert derived == "Please upgrade your Slack client to see this"
    assert "Approve" not in derived


def test_an_attachment_repeating_the_previous_segment_does_not_leak_its_fallback() -> None:
    """A duplicate is not an absence.

    https://docs.slack.dev/messaging/legacy-secondary-message-attachments/ --
    ``fallback`` is the plain-text summary Slack shows where the attachment
    cannot render, so it belongs in the derived prompt only when the attachment
    contributed nothing else. An alerting integration that posts the same line
    twice (a repeated status attached to two cards) is a payload where the second
    attachment plainly DID have content and its fallback must stay out.

    This is the interaction the two rules have with each other: the second
    attachment's ``text`` is identical to the segment immediately before it, so
    the adjacent-duplicate collapse leaves the emitted-segment list unchanged.
    Inferring "this attachment yielded nothing" from that unchanged list is what
    leaks the fallback, and it is the only shape in which the two rules can
    disagree.
    """
    event: dict[str, Any] = {
        "attachments": [
            {"text": "Checkout latency is back to normal"},
            {
                "text": "Checkout latency is back to normal",
                "fallback": "SHOULD_NOT_APPEAR_second_fallback",
            },
        ],
    }

    derived = derive_text(event)

    assert "SHOULD_NOT_APPEAR_second_fallback" not in derived
    # The repeated line is collapsed, so the whole derivation is that one line.
    assert derived == "Checkout latency is back to normal"


# ---------------------------------------------------------------------------
# UI-chrome key denylist
# ---------------------------------------------------------------------------


def test_ui_chrome_keys_are_denylisted() -> None:
    # https://docs.slack.dev/reference/block-kit/composition-objects/confirmation-dialog-object/
    # https://docs.slack.dev/reference/block-kit/block-elements/button-element/
    # https://docs.slack.dev/reference/block-kit/block-elements/static-select-element/
    # -- accessory, confirm, options, option_groups, placeholder, and hint
    # carry UI-only strings (button labels, dropdown choices, dialog copy)
    # that must never become model input; an actions block's own button
    # labels are denylisted the same way (D1).
    event: dict[str, Any] = {
        "blocks": [
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "Approve this request?"},
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "CHROME_ACCESSORY"},
                    "action_id": "btn1",
                    "confirm": {
                        "title": {"type": "plain_text", "text": "CHROME_CONFIRM_TITLE"},
                        "text": {"type": "mrkdwn", "text": "CHROME_CONFIRM_TEXT"},
                        "confirm": {"type": "plain_text", "text": "CHROME_CONFIRM_YES"},
                        "deny": {"type": "plain_text", "text": "CHROME_CONFIRM_NO"},
                    },
                },
            },
            {
                "type": "input",
                "block_id": "in1",
                "label": {"type": "plain_text", "text": "Real label text"},
                "hint": {"type": "plain_text", "text": "CHROME_HINT"},
                "element": {
                    "type": "static_select",
                    "placeholder": {"type": "plain_text", "text": "CHROME_PLACEHOLDER"},
                    "options": [
                        {
                            "text": {"type": "plain_text", "text": "CHROME_OPTION"},
                            "value": "v1",
                        }
                    ],
                    "option_groups": [
                        {
                            "label": {"type": "plain_text", "text": "unreachable group label"},
                            "options": [
                                {
                                    "text": {
                                        "type": "plain_text",
                                        "text": "CHROME_OPTION_GROUP",
                                    },
                                    "value": "v2",
                                }
                            ],
                        }
                    ],
                },
            },
            {
                "type": "actions",
                "block_id": "act1",
                "elements": [
                    {
                        "type": "button",
                        "text": {"type": "plain_text", "text": "CHROME_ACTION_BUTTON"},
                        "action_id": "approve1",
                    }
                ],
            },
        ],
    }

    derived = derive_text(event)

    for chrome in (
        "CHROME_ACCESSORY",
        "CHROME_CONFIRM_TITLE",
        "CHROME_CONFIRM_TEXT",
        "CHROME_CONFIRM_YES",
        "CHROME_CONFIRM_NO",
        "CHROME_HINT",
        "CHROME_PLACEHOLDER",
        "CHROME_OPTION",
        "CHROME_OPTION_GROUP",
        "CHROME_ACTION_BUTTON",
        "unreachable group label",
    ):
        assert chrome not in derived, f"chrome leaked: {chrome!r} in {derived!r}"

    # Positive control: real, non-chrome content in the same payload still
    # surfaces, so the assertions above are not vacuous.
    assert "Approve this request?" in derived
    assert "Real label text" in derived


# ---------------------------------------------------------------------------
# Unknown / future shapes -- accepted partial coverage (D1)
# ---------------------------------------------------------------------------


def test_unrecognized_block_type_still_yields_known_text_objects_at_depth() -> None:
    # The walker is generic and recursive: it finds mrkdwn/plain_text text
    # objects (https://docs.slack.dev/reference/block-kit/composition-objects/text-object/)
    # at any depth, including inside a block ``type`` it does not recognize
    # (D1) -- Slack ships new block types faster than any adapter can
    # special-case them.
    event: dict[str, Any] = {
        "blocks": [
            {
                "type": "workflow_step",  # not in D1's table
                "block_id": "ws1",
                "inputs": {
                    "note": {"type": "mrkdwn", "text": "buried inside an unknown block type"},
                },
            },
        ],
    }
    assert derive_text(event) == "buried inside an unknown block type"


def test_unrecognized_typed_node_with_str_text_yields_it() -> None:
    # Accepted partial coverage (D1): a node whose ``type`` is not in the
    # recognized table but whose own ``text`` value is a plain ``str`` (not a
    # nested text-object dict) still yields that string. D1 names Slack's
    # ``date`` rich-text element as exactly this case.
    event: dict[str, Any] = {
        "blocks": [
            {"type": "date", "text": "Jan 5", "timestamp": 1234567890},
        ],
    }
    assert derive_text(event) == "Jan 5"


def test_unrecognized_node_with_non_text_scalar_key_yields_nothing() -> None:
    # Accepted partial coverage, pinned rather than discovered later (D1): a
    # node carrying a *different* new scalar key (not ``text``) contributes
    # nothing, even though it is the only content in the payload.
    event: dict[str, Any] = {
        "blocks": [
            {"type": "future_block", "caption": "This should not surface"},
        ],
    }
    assert derive_text(event) == ""


# ---------------------------------------------------------------------------
# Bounds: traversal must stop, not just the output (D1)
# ---------------------------------------------------------------------------


def test_depth_cap_stops_traversal_but_keeps_shallower_content() -> None:
    # D1: max recursion depth _MAX_DEPTH; traversal must stop without
    # raising RecursionError, and must still return the segments found above
    # the cap.
    shallow = {"type": "mrkdwn", "text": "shallow content within the cap"}
    deep_leaf = {"type": "mrkdwn", "text": "buried past the depth cap"}
    event: dict[str, Any] = {
        "blocks": [shallow, _wrap_in_depth([deep_leaf], _MAX_DEPTH + 5)],
    }
    derived = derive_text(event)  # must not raise RecursionError
    assert "shallow content within the cap" in derived
    assert "buried past the depth cap" not in derived


def test_character_cap_truncates_and_traversal_stops() -> None:
    # D1: total character cap _MAX_CHARS. Truncating only the final joined
    # string would not bound a broad shallow payload -- traversal itself must
    # stop as soon as the cap is reached, so a sentinel placed after enough
    # bulk to exceed the cap must never appear in the output.
    bulk = [{"type": "mrkdwn", "text": "x" * 1000} for _ in range((_MAX_CHARS // 1000) + 5)]
    sentinel_block = {"type": "mrkdwn", "text": "SENTINEL_PAST_CHAR_CAP"}
    event: dict[str, Any] = {"blocks": [*bulk, sentinel_block]}
    derived = derive_text(event)
    assert len(derived) <= _MAX_CHARS
    assert "SENTINEL_PAST_CHAR_CAP" not in derived


def test_node_count_cap_bounds_a_broad_shallow_payload() -> None:
    # D1: max visited-node count _MAX_NODES. Depth 12 does not bound
    # breadth -- a flat, shallow list far past the node cap must still
    # return promptly and bounded, not visit (and join) every node.
    wide = [{"type": "mrkdwn", "text": f"seg{i}"} for i in range(_MAX_NODES * 4)]
    event: dict[str, Any] = {"blocks": wide}
    derived = derive_text(event)
    assert "seg0" in derived
    assert f"seg{_MAX_NODES * 4 - 1}" not in derived


def test_a_flat_list_of_bare_scalars_is_charged_against_the_node_budget() -> None:
    """Breadth spent on scalars must cost the same as breadth spent on dicts.

    https://docs.slack.dev/reference/block-kit/blocks/ -- ``blocks`` is a JSON
    array whose entries Slack defines as objects, so an array of bare strings and
    numbers is a payload only a hostile or broken sender produces. It must still
    be bounded: charging only dicts and lists lets a sender buy an unlimited
    number of visits for free and then place real content past the point the walk
    should have stopped.

    The sentinel is the oracle here, not the clock. It sits AFTER more scalars
    than the node budget allows, so a walk that charges every visited node can
    never reach it; a walk that charges only containers reaches it immediately.
    The elapsed-time assertion is the secondary guard against the same defect
    showing up as a stall rather than as leaked content.
    """
    scalars: list[Any] = ["filler", 12345] * 150_000
    sentinel = {"type": "mrkdwn", "text": "SENTINEL_PAST_THE_SCALAR_RUN"}
    event: dict[str, Any] = {"blocks": [*scalars, sentinel]}

    started = time.perf_counter()
    derived = derive_text(event)
    elapsed = time.perf_counter() - started

    assert "SENTINEL_PAST_THE_SCALAR_RUN" not in derived
    # Bare scalars are structure, not prose, so nothing at all is derived.
    assert derived == ""
    assert elapsed < 1.0, f"walking 300k bare scalars took {elapsed:.2f}s"


def test_an_attachment_with_an_enormous_fields_list_is_charged_per_field() -> None:
    """The container path must be charged too, not just ``blocks``.

    https://docs.slack.dev/messaging/legacy-secondary-message-attachments/ --
    ``fields`` is an array of ``{title, value}`` objects. An attachment carrying
    hundreds of thousands of empty ``{}`` fields is bounded only if the list AND
    each field are charged; otherwise the whole array is walked for free and the
    real field placed at its end is still reached.
    """
    fields: list[Any] = [{} for _ in range(200_000)]
    fields.append({"title": "SENTINEL_FIELD_TITLE", "value": "SENTINEL_FIELD_VALUE"})
    event: dict[str, Any] = {"attachments": [{"fields": fields}]}

    started = time.perf_counter()
    derived = derive_text(event)
    elapsed = time.perf_counter() - started

    assert "SENTINEL_FIELD_TITLE" not in derived
    assert "SENTINEL_FIELD_VALUE" not in derived
    assert derived == ""
    assert elapsed < 1.0, f"walking 200k empty attachment fields took {elapsed:.2f}s"


def test_a_single_enormous_text_value_is_trimmed_rather_than_kept_whole() -> None:
    """One huge string must not be stored, joined, or returned in full.

    https://docs.slack.dev/reference/block-kit/composition-objects/text-object/
    -- a ``mrkdwn`` text object's ``text`` is a single string, so a sender can
    put megabytes in ONE node. A cap applied only to the final joined result
    would still hold that whole string in memory on the way there; the visible
    consequence asserted here is that the derived prompt is exactly the cap and
    that the walk stopped, so nothing after the huge value is derived either.
    """
    huge = "z" * (5 * 1024 * 1024)
    event: dict[str, Any] = {
        "blocks": [
            {"type": "section", "text": {"type": "mrkdwn", "text": huge}},
            {"type": "section", "text": {"type": "mrkdwn", "text": "SENTINEL_PAST_HUGE_VALUE"}},
        ],
    }

    derived = derive_text(event)

    assert len(derived) == _MAX_CHARS
    assert derived == "z" * _MAX_CHARS
    assert "SENTINEL_PAST_HUGE_VALUE" not in derived


# ---------------------------------------------------------------------------
# files -- newly admitted via file_share (D3)
# ---------------------------------------------------------------------------


def test_files_contribute_title_or_name_per_file() -> None:
    # https://docs.slack.dev/reference/events/message.file_share -- D1: files
    # exist because D3 newly admits file_share; an admitted file share with
    # no derived text would be a brand-new silent-empty path, the exact
    # defect class this ticket closes.
    event: dict[str, Any] = {
        "subtype": "file_share",
        "text": "",
        "files": [
            {"id": "F1", "title": "quarterly-report.pdf", "name": "quarterly-report.pdf"},
            {"id": "F2", "name": "screenshot.png"},
        ],
    }
    derived = derive_text(event)
    assert "quarterly-report.pdf" in derived
    assert "screenshot.png" in derived


# ---------------------------------------------------------------------------
# Degenerate inputs -- must return "" without raising
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "event",
    [
        pytest.param({}, id="empty-event"),
        pytest.param({"text": ""}, id="empty-text"),
        pytest.param({"blocks": []}, id="empty-blocks-list"),
        pytest.param({"blocks": None}, id="blocks-none"),
        pytest.param({"attachments": [{}]}, id="attachment-empty-dict"),
        pytest.param({"blocks": ["not a dict"]}, id="block-is-bare-string"),
        pytest.param({"blocks": [None]}, id="none-inside-list"),
    ],
)
def test_degenerate_inputs_return_empty_string_without_raising(event: dict[str, Any]) -> None:
    assert derive_text(event) == ""
