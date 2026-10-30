"""ADR-0227: literal Slack delivery keeps the validated answer intact."""

import pytest
from channel_protocol import VERBATIM_REPLY_WIRE_VERSION
from channel_protocol.reply import ReplyTarget, ReplyUpdate
from curie_worker import slack_sink
from curie_worker.reply_sink import TargetRoute
from curie_worker.slack_sink import SlackReplyAdapter
from curie_worker.slack_text import SLACK_VERBATIM_MAX_BYTES, split_verbatim_answer
from slack_sdk.errors import SlackApiError

_ID = "00000000-0000-4000-8000-000000000001"


def _event(text: str, *, ref: str | None = "1.1") -> ReplyUpdate:
    return ReplyUpdate(
        version=VERBATIM_REPLY_WIRE_VERSION,
        event="reply.update",
        target=ReplyTarget(
            kind="slack", address="C0EXAMPLE1", conversation_id="1.0", reply_ref=ref
        ),
        delivery_id=_ID,
        text=text,
        text_mode="verbatim",
    )


def _assert_literal(call: dict[str, object]) -> str:
    blocks = call["blocks"]
    assert isinstance(blocks, list) and len(blocks) == 1
    # ADR 0250: an ordinary rich text section, not a preformatted code box.
    block = blocks[0]
    assert block["type"] == "rich_text"
    assert len(block["elements"]) == 1
    section = block["elements"][0]
    assert set(section) == {"type", "elements"}
    assert section["type"] == "rich_text_section"
    visible = []
    for element in section["elements"]:
        assert element["type"] in {"text", "link"}
        assert element["text"]
        if element["type"] == "link":
            assert set(element) == {"type", "url", "text"}
            assert element["url"] == element["text"]
        else:
            assert set(element) == {"type", "text"}
        visible.append(element["text"])
    text = call["text"]
    assert isinstance(text, str)
    assert "".join(visible) == text
    assert len(text.encode("utf-8")) <= SLACK_VERBATIM_MAX_BYTES
    return text


def _links(call: dict[str, object]) -> list[str]:
    blocks = call["blocks"]
    assert isinstance(blocks, list)
    return [
        element["url"]
        for element in blocks[0]["elements"][0]["elements"]
        if element["type"] == "link"
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    "text",
    [
        '```curie-reply\n{"text":"do not parse"}\n```',
        '```curie-reply\n{"text": malformed 🦊\n',
        "**markdown** <@U0EXAMPLE1> & > remain literal\n  ",
        "Summary\n\n- first *point*\n- second _point_\n    indented  twice\n\n\nEnd.",
    ],
)
async def test_verbatim_edit_preserves_structured_like_text(text: str) -> None:
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, object]] = []

    async def update(**kwargs: object) -> None:
        calls.append(kwargs)

    sink._client_for(None).chat_update = update  # type: ignore[method-assign]
    ack = await sink.emit(_event(text), route=TargetRoute())
    assert ack.ref == "1.1"
    assert len(calls) == 1
    assert _assert_literal(calls[0]) == text


@pytest.mark.anyio
async def test_verbatim_chunks_are_lossless_and_replay_post_keys_stable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    text = ("🦊é```\n" * 650) + "tail  "
    chunks = split_verbatim_answer(text)
    assert len(chunks) > 2
    assert "".join(part.content for part in chunks) == text
    delays: list[float] = []

    async def sleep(delay: float) -> None:
        delays.append(delay)

    monkeypatch.setattr(slack_sink, "_reply_sleep", sleep)
    sink = SlackReplyAdapter("xoxb-test")
    edits: list[dict[str, object]] = []
    posts: list[dict[str, object]] = []
    fail_once = True

    async def update(**kwargs: object) -> None:
        edits.append(kwargs)

    async def post(**kwargs: object) -> dict[str, str]:
        nonlocal fail_once
        posts.append(kwargs)
        if fail_once and len(posts) == 2:
            fail_once = False
            raise SlackApiError("ambiguous delivery", {"ok": False, "error": "internal_error"})
        return {"ts": f"2.{len(posts)}"}

    sink._client_for(None).chat_update = update  # type: ignore[method-assign]
    sink._client_for(None).chat_postMessage = post  # type: ignore[method-assign]
    event = _event(text)
    with pytest.raises(SlackApiError):
        await sink.emit(event, route=TargetRoute())
    first_attempt = [call["client_msg_id"] for call in posts]
    await sink.emit(event, route=TargetRoute())
    assert [call["client_msg_id"] for call in posts[2:4]] == first_attempt
    assert "".join(_assert_literal(call) for call in [edits[-1], *posts[2:]]) == text
    assert delays  # continuation posts retain the existing pacing contract


@pytest.mark.anyio
async def test_verbatim_rejected_blocks_do_not_fall_back_to_rendered_text() -> None:
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, object]] = []

    async def update(**kwargs: object) -> None:
        calls.append(kwargs)
        raise SlackApiError("invalid_blocks", {"ok": False, "error": "invalid_blocks"})

    sink._client_for(None).chat_update = update  # type: ignore[method-assign]
    with pytest.raises(SlackApiError):
        await sink.emit(_event("literal"), route=TargetRoute())
    assert len(calls) == 1
    assert _assert_literal(calls[0]) == "literal"


_REPORT = (
    "Weekly summary\n"
    "\n"
    "  Revenue: 1,234,567.89\n"
    "    - region A & region B (combined)\n"
    "\n"
    "\n"
    "Details: https://reports.example.com/view?a=1&b=2&name=Q3_(final)%28v2%29.\n"
    "Source (https://docs.example.com/page_(1)) and <https://x.example.com/p>, done\n"
    "*not bold* _not italic_ <@U0EXAMPLE1> :smile:\n"
)


@pytest.mark.anyio
async def test_verbatim_report_reads_as_a_section_with_exact_links() -> None:
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, object]] = []

    async def update(**kwargs: object) -> None:
        calls.append(kwargs)

    sink._client_for(None).chat_update = update  # type: ignore[method-assign]
    await sink.emit(_event(_REPORT), route=TargetRoute())
    assert len(calls) == 1
    assert _assert_literal(calls[0]) == _REPORT
    assert _links(calls[0]) == [
        "https://reports.example.com/view?a=1&b=2&name=Q3_(final)%28v2%29",
        "https://docs.example.com/page_(1)",
        "https://x.example.com/p",
    ]


@pytest.mark.anyio
async def test_verbatim_long_report_splits_on_lines_and_never_inside_a_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def sleep(delay: float) -> None:
        pass

    monkeypatch.setattr(slack_sink, "_reply_sleep", sleep)
    line = "  item é see https://reports.example.com/r?id=1&q=(x)%28y%29 now\n"
    text = "Header\n\n" + line * 120 + "\nEnd."
    assert len(text.encode("utf-8")) > 3000
    sink = SlackReplyAdapter("xoxb-test")
    calls: list[dict[str, object]] = []

    async def update(**kwargs: object) -> None:
        calls.append(kwargs)

    async def post(**kwargs: object) -> dict[str, str]:
        calls.append(kwargs)
        return {"ts": f"2.{len(calls)}"}

    sink._client_for(None).chat_update = update  # type: ignore[method-assign]
    sink._client_for(None).chat_postMessage = post  # type: ignore[method-assign]
    await sink.emit(_event(text), route=TargetRoute())
    assert len(calls) > 1
    parts = [_assert_literal(call) for call in calls]
    assert "".join(parts) == text
    assert all(part.endswith("\n") for part in parts[:-1])
    url = "https://reports.example.com/r?id=1&q=(x)%28y%29"
    assert all(set(_links(call)) == {url} for call in calls)
    assert sum(len(_links(call)) for call in calls) == 120


def test_verbatim_split_keeps_a_url_whole_without_a_line_break() -> None:
    url = "https://reports.example.com/" + "a" * 200
    text = ("word " * 540) + url + (" tail" * 10)
    parts = split_verbatim_answer(text)
    assert "".join(part.content for part in parts) == text
    assert any(url in part.content for part in parts)
    assert all(len(part.content.encode("utf-8")) <= SLACK_VERBATIM_MAX_BYTES for part in parts)
