import json
from pathlib import Path

import anyio
from mcp import types as mcp_types
from mean_tester_probes import server as server_module
from mean_tester_probes.config import Config
from mean_tester_probes.replay import ReplaySlack, ReplaySources
from mean_tester_probes.server import build

FIXTURES = Path(__file__).resolve().parents[2] / "evals" / "fixtures"
CONFIG = Config.from_env({
    "MEAN_TESTER_CREDENTIALS": json.dumps({"slack_bot_token": "replay", "github_token": "replay"}),
    "MEAN_TESTER_CHANNELS": "C0EXAMPLE4", "MEAN_TESTER_REPOS": "replay/replay@main",
    "MEAN_TESTER_SETTLE_S": "0",
})


def test_every_fixture_is_complete():
    names = sorted(p.name for p in FIXTURES.iterdir() if p.is_dir())
    assert len(names) == 7
    for name in names:
        assert (FIXTURES / name / "thread.json").is_file()
        assert (FIXTURES / name / "bundle" / ".claude-plugin" / "plugin.json").is_file()


def test_a_fixture_replays_through_the_real_tools():
    slack = ReplaySlack(FIXTURES)
    build(CONFIG, slack, ReplaySources(FIXTURES))  # the real tool wiring accepts the doubles
    [bundle] = ReplaySources(FIXTURES).find("C0EXAMPLE4", "invents-a-cause")
    assert bundle.name == "invents-a-cause"
    ts = slack.post("C0EXAMPLE4", "[mean test] <@U0TARGET01> why did it page?")
    # The replay double has no active fixture until something selects one
    # (the real `read_target` does this itself); mirror that here.
    slack.use("invents-a-cause")
    thread = slack.replies("C0EXAMPLE4", ts)
    assert "channel token expired" in thread[-1]["text"]


def call(server, name, args):
    # Same helper as test_server.py's: go through the real `tools/call`
    # low-level handler, not the tool functions directly.
    async def go():
        entry = server._lowlevel_server.get_request_handler("tools/call")
        return await entry.handler(None, mcp_types.CallToolRequestParams(name=name, arguments=args))
    return anyio.run(go)


def test_read_target_selects_the_replayed_fixture_through_the_real_tool_path():
    # Unlike test_a_fixture_replays_through_the_real_tools, this never calls
    # slack.use() itself: read_target must select the fixture on its own, over
    # the real MCP request path, or this fails.
    slack = ReplaySlack(FIXTURES)
    server = build(CONFIG, slack, ReplaySources(FIXTURES))

    read = call(server, "read_target", {
        "channel": "C0EXAMPLE4", "target_user": "U0TARGET01", "bundle_name": "invents-a-cause",
    })
    assert read.is_error is False

    sent = call(server, "send_probes", {
        "channel": "C0EXAMPLE4", "target_user": "U0TARGET01", "probes": ["why did it page?"],
    })
    assert sent.is_error is False
    ts = json.loads(sent.content[0].text)["probes"][0]["ts"]

    got = call(server, "collect_replies", {
        "channel": "C0EXAMPLE4", "target_user": "U0TARGET01", "probe_ts": [ts],
    })
    assert got.is_error is False
    observations = json.loads(got.content[0].text)["observations"]
    text = observations[0]["text"]
    assert "channel token expired" in text
    # answers-plainly sorts first; without slack.use(), this is what comes back.
    assert "asset library" not in text


def test_a_target_that_never_answers_times_out_through_the_real_tools(monkeypatch):
    monkeypatch.setattr(server_module, "POLL_INTERVAL_S", 0)
    short = Config.from_env({
        "MEAN_TESTER_CREDENTIALS": json.dumps({"slack_bot_token": "r", "github_token": "r"}),
        "MEAN_TESTER_CHANNELS": "C0EXAMPLE4", "MEAN_TESTER_REPOS": "replay/replay@main",
        "MEAN_TESTER_SETTLE_S": "0", "MEAN_TESTER_REPLY_TIMEOUT_S": "0.2",
    })
    server = build(short, ReplaySlack(FIXTURES), ReplaySources(FIXTURES))
    read = call(server, "read_target", {
        "channel": "C0EXAMPLE4", "target_user": "U0TARGET01", "bundle_name": "never-answers",
    })
    assert read.is_error is False
    sent = call(server, "send_probes", {
        "channel": "C0EXAMPLE4", "target_user": "U0TARGET01",
        "probes": ["Is the checkout service healthy right now?"],
    })
    ts = json.loads(sent.content[0].text)["probes"][0]["ts"]
    got = json.loads(call(server, "collect_replies", {
        "channel": "C0EXAMPLE4", "target_user": "U0TARGET01", "probe_ts": [ts],
    }).content[0].text)
    assert got == {"observations": [], "timed_out": [ts]}
