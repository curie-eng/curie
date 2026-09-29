"""Markdown -> Slack mrkdwn conversion (the chat.update render fix)."""

from __future__ import annotations

import pytest
from curie_worker.mrkdwn import to_mrkdwn


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        pytest.param("**bold** here", "*bold* here", id="bold_double_asterisk_becomes_single"),
        pytest.param(
            "see [the docs](https://x.com/a)",
            "see <https://x.com/a|the docs>",
            id="link_becomes_angle_pipe",
        ),
        pytest.param("## Results", "*Results*", id="h2_heading_becomes_bold_line"),
        pytest.param("# Top\n### Deep", "*Top*\n*Deep*", id="h1_and_h3_headings_become_bold_lines"),
        pytest.param("## Title ##", "*Title*", id="heading_with_trailing_hashes_is_stripped"),
        pytest.param("- one\n- two", "• one\n• two", id="dash_bullet_becomes_bullet_char"),
        pytest.param("* one\n* two", "• one\n• two", id="asterisk_bullet_becomes_bullet_char"),
        pytest.param(
            "- top\n  - nested", "• top\n  • nested", id="nested_bullet_preserves_indentation"
        ),
        pytest.param(
            "list:\n```\n- code bullet\n* also code\n```",
            "list:\n```\n- code bullet\n* also code\n```",
            id="bullet_inside_fenced_code_is_unchanged",
        ),
        # A leading `*` with no trailing space is italic, not a bullet: left alone.
        pytest.param("*em* text", "*em* text", id="italic_line_is_not_a_bullet"),
        pytest.param(
            "1. first\n2. second", "1. first\n2. second", id="numbered_list_is_left_alone"
        ),
        pytest.param(
            "## Plan\n- **do** this\n  - see [docs](http://z)\n```\n- literal\n```",
            "*Plan*\n• *do* this\n  • see <http://z|docs>\n```\n- literal\n```",
            id="mixed_doc_converts_bullets_bold_and_leaves_code",
        ),
        # ** and [](...) inside a code span must not be rewritten.
        pytest.param(
            "use `**not bold** [x](y)`",
            "use `**not bold** [x](y)`",
            id="inline_code_is_preserved_verbatim",
        ),
        pytest.param(
            "before\n```\n**still code** [x](y)\n## nope\n```\nafter **bold**",
            "before\n```\n**still code** [x](y)\n## nope\n```\nafter *bold*",
            id="fenced_code_block_is_preserved_verbatim",
        ),
        pytest.param(
            "## Summary\n**Done**: see [here](http://z)",
            "*Summary*\n*Done*: see <http://z|here>",
            id="combined_bold_link_and_heading",
        ),
        pytest.param("", "", id="empty_string_is_unchanged"),
        pytest.param(
            "just a normal sentence.", "just a normal sentence.", id="plain_text_is_unchanged"
        ),
        pytest.param("#hashtag stays", "#hashtag stays", id="hash_without_space_is_not_a_heading"),
    ],
)
def test_to_mrkdwn(text: str, expected: str) -> None:
    assert to_mrkdwn(text) == expected
