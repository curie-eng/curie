"""Answer delivery after Slack refuses a part with msg_too_long.

Slack documents msg_too_long for both methods the answer path calls:
https://docs.slack.dev/reference/methods/chat.update/ and
https://docs.slack.dev/reference/methods/chat.postMessage/ . The double below
refuses any text over a byte limit the way the SDK raises it, so the stored
form growing past the bytes sent is modelled by a limit under 4000.
"""

import asyncio

import pytest
from channel_protocol.reply import (
    PROGRESS_REPLY_WIRE_VERSION,
    VERBATIM_REPLY_WIRE_VERSION,
    ReplyTarget,
    ReplyUpdate,
)
from curie_worker import reply_sink, slack_sink
from curie_worker.blocks import render
from curie_worker.reply_sink import ObservedReplySink, TargetRoute
from curie_worker.slack_sink import SlackReplyAdapter
from curie_worker.slack_text import split_answer, split_verbatim_answer
from slack_sdk.errors import SlackApiError
from slack_sdk.web.async_slack_response import AsyncSlackResponse

# The fixed notice the issue pins, written out so the test checks the contract text.
MSG_TOO_LONG_NOTICE = (
    "Slack refused part of this reply as too long (msg_too_long), so that part is not shown here."
)

DELIVERY_ID = "00000000-0000-4000-8000-000000000157"


@pytest.fixture(autouse=True)
def sleeps(monkeypatch):
    seen = []

    async def sleep(delay):
        seen.append(delay)

    monkeypatch.setattr(slack_sink, "_reply_sleep", sleep)
    return seen


@pytest.fixture
def outcomes(monkeypatch):
    seen = []

    def record(name, *_args, **kwargs):
        seen.append((name, (kwargs.get("attributes") or {}).get("outcome")))

    monkeypatch.setattr(reply_sink.curie_telemetry, "record_metric", record)
    return seen


def refusal(error="msg_too_long", status_code=200, headers=None):
    return SlackApiError(
        "refused",
        AsyncSlackResponse(
            client=None,
            http_verb="POST",
            api_url="https://slack.com/api/chat.update",
            req_args={},
            data={"ok": False, "error": error},
            headers=headers or {},
            status_code=status_code,
        ),
    )


def event(text, mode="mrkdwn", ref="1.2"):
    verbatim = mode == "verbatim"
    return ReplyUpdate(
        version=VERBATIM_REPLY_WIRE_VERSION if verbatim else PROGRESS_REPLY_WIRE_VERSION,
        event="reply.update",
        target=ReplyTarget(
            kind="slack", address="C0EXAMPLE1", conversation_id="1.1", reply_ref=ref
        ),
        delivery_id=DELIVERY_ID,
        text=text,
        text_mode="verbatim" if verbatim else None,
    )


def prose(size):
    # Numbered lines keep every smaller part's text distinct.
    text = "".join(
        f"Line {n}: plain prose with spaces, naïve café text, and no fences.\n"
        for n in range(size // 40)
    )
    return text.encode()[:size].decode("utf-8", "ignore")


def rendered(text, mode):
    return text if mode == "verbatim" else render(text, None)[0]


class Slack:
    """Slack double that refuses text over `limit` and dedupes creates by key."""

    def __init__(self, sink, limit, created=None):
        self.limit = limit
        self.calls = []
        self.created = {} if created is None else created
        self.fail_index = None
        self.failed = False
        self.first_error = None
        client = sink._client_for(None)
        client.chat_update = self.update
        client.chat_postMessage = self.post

    def _check(self, method, body):
        self.calls.append((method, body))
        if self.first_error is not None:
            error, self.first_error = self.first_error, None
            raise error
        if len(body["text"].encode()) > self.limit:
            raise refusal()

    def _deduped(self, body):
        # Slack dedupes chat.postMessage by client_msg_id and answers ok with the
        # FIRST message's ts even when the text differs (the observation recorded in
        # SlackReplyAdapter._adopt_posted_ts). That happens before any length check.
        key = body.get("client_msg_id")
        if key in self.created:
            self.calls.append(("post-dedupe", body))
            return {"ok": True, "ts": self.created[key]["ts"]}
        return None

    async def update(self, **body):
        self._check("update", body)
        self.calls[-1] = ("update-ok", body)
        return {"ok": True, "ts": body["ts"]}

    async def post(self, **body):
        deduped = self._deduped(body)
        if deduped is not None:
            return deduped
        self._check("post", body)
        self.calls[-1] = ("post-ok", body)
        key = body["client_msg_id"]
        if key not in self.created:
            self.created[key] = dict(body, ts=f"2.{len(self.created) + 1}")
        if len(self.created) == self.fail_index and not self.failed:
            self.failed = True
            raise TimeoutError("synthetic lost acknowledgement")
        return {"ok": True, "ts": self.created[key]["ts"]}

    def delivered(self):
        return [body["text"] for method, body in self.calls if method.endswith("-ok")]


async def emit(sink, reply):
    return await ObservedReplySink(sink).emit(reply, route=TargetRoute())


@pytest.mark.parametrize("size", [3000, 12000])
@pytest.mark.parametrize("mode", ["mrkdwn", "verbatim"])
def test_msg_too_long_part_is_redelivered_as_smaller_parts(mode, size, outcomes):
    async def run():
        text = prose(size)
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink, limit=2000)
        await emit(sink, event(text, mode))
        delivered = slack.delivered()
        assert slack.calls[0][0] == "update"
        assert [m for m, _ in slack.calls if m.endswith("-ok")][0] == "update-ok"
        assert "".join(delivered) == rendered(text, mode)
        if mode == "verbatim":
            originals = [p.content for p in split_verbatim_answer(text)]
        else:
            originals = [p.text for p in split_answer(rendered(text, mode))]
        accepted_whole = {o for o in originals if len(o.encode()) <= 2000}
        # Every accepted part of a refused original honours the 800 byte resend budget.
        assert all(len(t.encode()) <= 800 for t in delivered if t not in accepted_whole)
        assert len(accepted_whole) < len(originals)  # some part was actually refused
        assert MSG_TOO_LONG_NOTICE not in delivered
        assert ("curie.reply.delivery", "success") in outcomes
        assert ("curie.reply.delivery", "failure") not in outcomes

    asyncio.run(run())


@pytest.mark.parametrize("mode", ["mrkdwn", "verbatim"])
def test_refused_smaller_part_is_replaced_by_one_notice_per_part(mode, outcomes, caplog):
    async def run():
        text = prose(9000)
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink, limit=300)
        await emit(sink, event(text, mode))
        if mode == "verbatim":
            originals = len(split_verbatim_answer(text))
        else:
            originals = len(split_answer(render(text, None)[0]))
        assert originals > 1
        assert slack.delivered().count(MSG_TOO_LONG_NOTICE) == originals
        assert slack.delivered() == [MSG_TOO_LONG_NOTICE] * originals
        assert ("curie.reply.delivery", "success") in outcomes
        assert DELIVERY_ID in caplog.text

    asyncio.run(run())


def test_retry_after_partial_smaller_delivery_reuses_every_smaller_key():
    async def run():
        text = prose(3000)
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink, limit=2000)
        # The second smaller post records its create, then loses the ack.
        slack.fail_index = 2
        with pytest.raises(TimeoutError):
            await emit(sink, event(text))
        first_keys = [body["client_msg_id"] for method, body in slack.calls if "post" in method]
        retry_sink = SlackReplyAdapter("xoxb-test")
        retry = Slack(retry_sink, limit=2000, created=slack.created)
        await emit(retry_sink, event(text))
        retry_keys = [body["client_msg_id"] for method, body in retry.calls if "post" in method]
        assert len(first_keys) == 2
        assert retry_keys[: len(first_keys)] == first_keys
        assert len(retry_keys) == len(set(retry_keys))
        texts = [body["text"] for body in slack.created.values()]
        assert len(texts) == len(set(texts))
        placeholder = next(body["text"] for m, body in retry.calls if m == "update-ok")
        assert placeholder + "".join(texts) == rendered(text, "mrkdwn")

    asyncio.run(run())


@pytest.mark.parametrize("reply_ref", ["1.2", None])
@pytest.mark.parametrize("mode", ["mrkdwn", "verbatim"])
def test_retry_after_lost_ack_on_later_smaller_piece_does_not_truncate(mode, reply_ref, outcomes):
    """A dedupe on the refused part's own key must not swallow the retry."""

    async def run():
        text = prose(12000)
        if mode == "verbatim":
            parts = [p.content for p in split_verbatim_answer(text)]
        else:
            parts = [p.text for p in split_answer(render(text, None)[0])]
        budget = slack_sink._MSG_TOO_LONG_RESEND_BYTES

        def pieces(part):
            if mode == "verbatim":
                return [s.content for s in split_verbatim_answer(part, max_bytes=budget)]
            return [s.text for s in split_answer(part, max_bytes=budget)]

        assert len(parts) >= 3
        assert all(len(p.encode()) > 2000 for p in parts[:2])
        # Creates before part 1: part 0's pieces, minus the placeholder edit.
        before = len(pieces(parts[0])) - (1 if reply_ref is not None else 0)
        assert len(pieces(parts[1])) >= 2
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink, limit=2000)
        # Lose the ack on part 1's second smaller piece, after piece 0 was accepted.
        slack.fail_index = before + 2
        with pytest.raises(TimeoutError):
            await emit(sink, event(text, mode, ref=reply_ref))
        retry_sink = SlackReplyAdapter("xoxb-test")
        retry = Slack(retry_sink, limit=2000, created=slack.created)
        await emit(retry_sink, event(text, mode, ref=reply_ref))
        texts = [body["text"] for body in retry.created.values()]
        # A part within the limit (the short last one) is accepted whole.
        expected = [
            piece
            for part in parts
            for piece in (pieces(part) if len(part.encode()) > 2000 else [part])
        ]
        placeholder = (
            [next(body["text"] for m, body in slack.calls if m == "update-ok")]
            if reply_ref is not None
            else []
        )
        assert placeholder + texts == expected
        assert len(texts) == len(set(texts))
        assert "".join(placeholder + texts) == rendered(text, mode)
        assert ("curie.reply.delivery", "success") in outcomes

    asyncio.run(run())


def test_other_slack_errors_still_raise_without_resend(outcomes):
    async def run():
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink, limit=4000)
        slack.first_error = refusal("channel_not_found")
        with pytest.raises(SlackApiError):
            await emit(sink, event(prose(3000)))
        assert len(slack.calls) == 1
        assert ("curie.reply.delivery", "failure") in outcomes

    asyncio.run(run())


def test_rate_limit_is_retried_after_retry_after(sleeps, outcomes):
    async def run():
        text = prose(3000)
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink, limit=4000)
        slack.first_error = refusal("ratelimited", 429, {"Retry-After": "1"})
        await emit(sink, event(text))
        assert 1.0 in sleeps
        assert slack.delivered() == [rendered(text, "mrkdwn")]
        assert ("curie.reply.delivery", "success") in outcomes

    asyncio.run(run())


def test_answer_under_the_limit_is_one_update():
    async def run():
        text = prose(1500)
        sink = SlackReplyAdapter("xoxb-test")
        slack = Slack(sink, limit=2000)
        await emit(sink, event(text))
        assert [method for method, _ in slack.calls] == ["update-ok"]
        assert slack.delivered() == [rendered(text, "mrkdwn")]

    asyncio.run(run())


class LostNoticeAckSlack(Slack):
    """Stores the first created notice, then loses its acknowledgement once."""

    lost = False

    async def post(self, **body):
        result = await super().post(**body)
        if body["text"] == MSG_TOO_LONG_NOTICE and not self.lost:
            self.lost = True
            raise TimeoutError("synthetic lost acknowledgement")
        return result


@pytest.mark.parametrize("reply_ref", ["1.2", None])
@pytest.mark.parametrize("mode", ["mrkdwn", "verbatim"])
def test_retry_after_lost_notice_ack_posts_one_notice_per_refused_part(mode, reply_ref, outcomes):
    async def run():
        text = prose(9000)
        if mode == "verbatim":
            originals = [p.content for p in split_verbatim_answer(text)]
        else:
            originals = [p.text for p in split_answer(render(text, None)[0])]
        refused = [o for o in originals if len(o.encode()) > 300]
        assert len(refused) >= 2
        sink = SlackReplyAdapter("xoxb-test")
        slack = LostNoticeAckSlack(sink, limit=300)
        with pytest.raises(TimeoutError):
            await emit(sink, event(text, mode, ref=reply_ref))
        assert slack.lost
        retry_sink = SlackReplyAdapter("xoxb-test")
        retry = LostNoticeAckSlack(retry_sink, limit=300, created=slack.created)
        retry.lost = True
        await emit(retry_sink, event(text, mode, ref=reply_ref))
        created = [body["text"] for body in retry.created.values()]
        edits = [body["text"] for m, body in retry.calls if m == "update-ok"]
        # The placeholder edit, when there is one, is the last edit of the retry.
        shown = created + edits[-1:] if reply_ref is not None else created
        assert shown.count(MSG_TOO_LONG_NOTICE) == len(refused)
        for text_shown in shown:
            assert text_shown == MSG_TOO_LONG_NOTICE or text_shown in originals
            assert text_shown not in refused
        assert ("curie.reply.delivery", "success") in outcomes

    asyncio.run(run())
