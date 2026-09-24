import httpx
import pytest
from mean_tester_probes.slack import SlackApi, SlackError


def api(handler):
    return SlackApi("xoxb-test", client=httpx.Client(transport=httpx.MockTransport(handler)))


def test_post_sends_to_the_channel_root_and_returns_the_ts():
    seen = {}

    def handler(request):
        seen["url"] = str(request.url)
        seen["auth"] = request.headers["authorization"]
        seen["body"] = request.read()
        return httpx.Response(200, json={"ok": True, "ts": "1790000000.000100"})

    assert api(handler).post("C0EXAMPLE2", "[mean test] hi") == "1790000000.000100"
    assert seen["url"] == "https://slack.com/api/chat.postMessage"
    assert seen["auth"] == "Bearer xoxb-test"
    assert b"thread_ts" not in seen["body"]  # a root post opens a new thread (d1)


def test_ok_false_is_an_error_not_an_empty_result():
    with pytest.raises(SlackError, match="not_in_channel"):
        refusal = httpx.Response(200, json={"ok": False, "error": "not_in_channel"})
        api(lambda r: refusal).post("C1", "x")


def test_replies_reads_the_whole_thread():
    def handler(request):
        assert request.url.params["channel"] == "C1" and request.url.params["ts"] == "1.0"
        return httpx.Response(200, json={"ok": True, "messages": [{"ts": "1.0"}, {"ts": "2.0"}]})

    assert [m["ts"] for m in api(handler).replies("C1", "1.0")] == ["1.0", "2.0"]
