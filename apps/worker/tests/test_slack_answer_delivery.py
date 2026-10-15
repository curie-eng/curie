"""Lossless terminal answer delivery against a synthetic Slack API.

The 4000 UTF-8 byte safety limit is pinned by the observed edit rejection
record in test_slack_sink.py. Slack documents threaded creates and text/blocks
behavior at https://docs.slack.dev/reference/methods/chat.postMessage/ and
https://docs.slack.dev/reference/methods/chat.update/. These fixtures do not
claim current live Slack qualification.
"""

import asyncio

import pytest
from channel_protocol.reply import PROGRESS_REPLY_WIRE_VERSION, ReplyTarget, ReplyUpdate
from curie_worker.reply_sink import TargetRoute
from curie_worker.slack_sink import SlackReplyAdapter


@pytest.fixture(autouse=True)
def _instant_synthetic_slack_delays(monkeypatch):
    from curie_worker import slack_sink

    async def sleep(_delay):
        return None

    monkeypatch.setattr(slack_sink, "_reply_sleep", sleep)


def event(text, ref="1.2"):
    return ReplyUpdate(
        version=PROGRESS_REPLY_WIRE_VERSION,
        event="reply.update",
        target=ReplyTarget(
            kind="slack", address="C0EXAMPLE1", conversation_id="1.1", reply_ref=ref
        ),
        delivery_id="00000000-0000-4000-8000-000000000071",
        text=text,
    )


class Slack:
    """External service double, including ambiguous create deduplication."""

    def __init__(self, sink):
        self.calls = []
        self.created = {}
        self.fail_index = None
        self.failed = False
        client = sink._client_for(None)
        client.chat_update = self.update
        client.chat_postMessage = self.post

    async def update(self, **body):
        assert len(body["text"].encode()) <= 4000
        self.calls.append(("update", body))
        return {"ok": True, "ts": body["ts"]}

    async def post(self, **body):
        assert len(body["text"].encode()) <= 4000
        self.calls.append(("post", body))
        key = body["client_msg_id"]
        if key not in self.created:
            self.created[key] = dict(body, ts=f"2.{len(self.created) + 1}")
        if len(self.created) == self.fail_index and not self.failed:
            self.failed = True
            raise TimeoutError("synthetic lost acknowledgement")
        return {"ok": True, "ts": self.created[key]["ts"]}


@pytest.mark.parametrize(
    "text",
    [
        "x" * 4001,
        ("한글 e\u0301 👩\u200d💻\n" * 1200),
        ("paragraph one\n\nparagraph two\n" * 1000),
    ],
    ids=["ascii", "unicode", "paragraphs"],
)
def test_oversized_answer_is_sequential_and_lossless(text):
    async def run():
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink)
        ack = await sink.emit(event(text), route=TargetRoute())
        assert ack.ref == "1.2"
        assert len(slack.calls) > 1
        assert slack.calls[0][0] == "update"
        assert all(method == "post" for method, _ in slack.calls[1:])
        assert "".join(body["text"] for _, body in slack.calls) == text
        assert all(body["thread_ts"] == "1.1" for _, body in slack.calls[1:])
        assert len({body["client_msg_id"] for _, body in slack.calls[1:]}) == len(slack.calls) - 1
        assert all("Cut here" not in body["text"] for _, body in slack.calls)

    asyncio.run(run())


def test_partial_delivery_raises_and_retry_reuses_all_chunk_keys():
    async def run():
        text = "first\n" * 2000
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink)
        slack.fail_index = 2
        with pytest.raises(TimeoutError):
            await sink.emit(event(text), route=TargetRoute())
        first_keys = [body["client_msg_id"] for method, body in slack.calls if method == "post"]
        # A fresh adapter models worker restart; the external service keeps its
        # accepted creates. No in-process delivery cache can satisfy this test.
        retry_sink = SlackReplyAdapter("xoxb-test")
        retry_sink._client_for(None).chat_update = slack.update
        retry_sink._client_for(None).chat_postMessage = slack.post
        await retry_sink.emit(event(text), route=TargetRoute())
        retry_keys = [body["client_msg_id"] for method, body in slack.calls if method == "post"][
            len(first_keys) :
        ]
        assert retry_keys[: len(first_keys)] == first_keys
        assert (
            "".join([slack.calls[0][1]["text"]] + [body["text"] for body in slack.created.values()])
            == text
        )

    asyncio.run(run())


def test_placeholderless_long_reply_adopts_first_message_and_stays_in_thread():
    async def run():
        text = "answer\n" * 2000
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink)
        ack = await sink.emit(event(text, ref=None), route=TargetRoute())
        assert ack.ref == "2.1"
        assert all(method == "post" for method, _ in slack.calls)
        assert all(body["thread_ts"] == "1.1" for _, body in slack.calls)
        assert "".join(body["text"] for _, body in slack.calls) == text

    asyncio.run(run())


def test_link_and_inline_code_are_not_split_at_the_byte_boundary():
    async def run():
        atom = "<https://example.com/report|the report> `short code`"
        text = "x" * 3980 + "\n" + atom + "\n" + "tail" * 1200
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink)
        await sink.emit(event(text), route=TargetRoute())
        assert sum(atom in body["text"] for _, body in slack.calls) == 1
        assert "".join(body["text"] for _, body in slack.calls) == text

    asyncio.run(run())


def test_long_code_fence_reopens_in_each_message_without_losing_body():
    async def run():
        text = "```\n" + "synthetic code line\n" * 1400 + "```"
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink)
        await sink.emit(event(text), route=TargetRoute())
        sent = [body["text"] for _, body in slack.calls]
        assert len(sent) > 2
        assert all(part.count("```") == 2 for part in sent)
        restored = sent[0].removesuffix("\n```")
        restored += "".join(part.removeprefix("```\n").removesuffix("\n```") for part in sent[1:-1])
        restored += sent[-1].removeprefix("```\n")
        assert restored == text

    asyncio.run(run())


def test_distinct_turn_ids_never_share_continuation_keys():
    async def run():
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink)
        text = "same answer" * 900
        first = event(text)
        await sink.emit(first, route=TargetRoute())
        initial = set(slack.created)
        second = first.model_copy(update={"delivery_id": "00000000-0000-4000-8000-000000000072"})
        await sink.emit(second, route=TargetRoute())
        assert len(slack.created) == 2 * len(initial)

    asyncio.run(run())


def test_oversized_structured_reply_keeps_its_header_footer_and_complete_body():
    import json

    async def run():
        body = "requested line\n" * 3000
        text = (
            "```curie-reply\n"
            + json.dumps(
                {"header": "Requested report", "text": body, "footer": "Synthetic observation"}
            )
            + "\n```"
        )
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink)
        await sink.emit(event(text), route=TargetRoute())
        assert "".join(payload["text"] for _, payload in slack.calls) == body
        blocks = [block for _, payload in slack.calls for block in payload.get("blocks", [])]
        headers = [block["text"]["text"] for block in blocks if block["type"] == "header"]
        footers = [block["elements"][0]["text"] for block in blocks if block["type"] == "context"]
        assert headers == ["Requested report"]
        assert footers == ["Synthetic observation"]

    asyncio.run(run())


def test_hook_answer_continuations_use_existing_message_as_the_thread_root():
    async def run():
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink)
        reply = event("hook result\n" * 1200)
        reply = reply.model_copy(
            update={
                "target": reply.target.model_copy(
                    update={"conversation_id": "hook:acme-bot:report"}
                )
            }
        )
        await sink.emit(reply, route=TargetRoute())
        continuations = [body for method, body in slack.calls if method == "post"]
        assert continuations
        assert all(body["thread_ts"] == "1.2" for body in continuations)

    asyncio.run(run())


@pytest.mark.parametrize("header_name", ["Retry-After", "retry-after"])
def test_continuations_honor_retry_after_and_preserve_all_content(monkeypatch, header_name):
    from curie_worker import slack_sink
    from slack_sdk.errors import SlackApiError

    delays = []

    async def sleep(delay):
        delays.append(delay)

    monkeypatch.setattr(slack_sink, "_reply_sleep", sleep, raising=False)

    async def run():
        text = "requested line\n" * 1400
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink)
        original_post = slack.post
        refused = 0

        async def rate_limited(**body):
            nonlocal refused
            if len(slack.created) == 1 and refused < 2:
                refused += 1
                raise SlackApiError(
                    "synthetic rate limit",
                    {
                        "error": "ratelimited",
                        "status_code": 429,
                        "headers": {header_name: "2"},
                    },
                )
            return await original_post(**body)

        sink._client_for(None).chat_postMessage = rate_limited
        await sink.emit(event(text), route=TargetRoute())
        assert refused == 2
        assert delays.count(2.0) == 2
        assert "".join(body["text"] for _, body in slack.calls) == text

    asyncio.run(run())


@pytest.mark.parametrize(
    "text",
    ["x" * 3992 + "🇺🇸" + "tail" * 10, "a" + "\u0301" * 2500],
    ids=["flag", "oversized-cluster"],
)
def test_unicode_boundaries_are_lossless_even_for_oversized_clusters(text):
    from curie_worker.slack_text import split_answer

    parts = split_answer(text)
    assert "".join(part.content for part in parts) == text
    assert all(len(part.text.encode()) <= 4000 for part in parts)
    if "🇺🇸" in text:
        assert sum("🇺🇸" in part.content for part in parts) == 1
