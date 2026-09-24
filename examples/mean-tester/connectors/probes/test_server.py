import json
import threading

import anyio
from mcp import types as mcp_types
from mean_tester_probes.config import Config, RepoRef
from mean_tester_probes.server import build
from mean_tester_probes.slack import SlackError
from mean_tester_probes.sources import BundleSource

CONFIG = Config.from_env({
    "MEAN_TESTER_CREDENTIALS": '{"slack_bot_token": "x", "github_token": "y"}',
    "MEAN_TESTER_CHANNELS": "C0EXAMPLE2", "MEAN_TESTER_REPOS": "acme/agents@main",
    "MEAN_TESTER_SETTLE_S": "0", "MEAN_TESTER_REPLY_TIMEOUT_S": "1",
})
TWO_REPOS = Config.from_env({
    "MEAN_TESTER_CREDENTIALS": '{"slack_bot_token": "x", "github_token": "y"}',
    "MEAN_TESTER_CHANNELS": "C0EXAMPLE2,C0EXAMPLE3",
    "MEAN_TESTER_REPOS": "acme/agents@main,acme/tools@main",
    "MEAN_TESTER_SETTLE_S": "0", "MEAN_TESTER_REPLY_TIMEOUT_S": "1",
})
# A long window, so the rate tests can step a fake clock across it.
WINDOWED = Config.from_env({
    "MEAN_TESTER_CREDENTIALS": '{"slack_bot_token": "x", "github_token": "y"}',
    "MEAN_TESTER_CHANNELS": "C0EXAMPLE2", "MEAN_TESTER_REPOS": "acme/agents@main",
    "MEAN_TESTER_REPLY_TIMEOUT_S": "240",
})
TARGET = "U0TARGET01"


class FakeSlack:
    def __init__(self, members=(TARGET,)):
        self.posted = []
        self._members = set(members)

    def channel_info(self, channel):
        return {"id": channel, "is_ext_shared": False, "is_shared": False}

    def members(self, channel):
        return set(self._members)

    def post(self, channel, text):
        self.posted.append(text)
        return f"1790000000.00010{len(self.posted)}"

    def replies(self, channel, ts):
        return [{"ts": ts, "user": "U0TESTER01", "text": "probe"},
                {"ts": "1790000001.0", "user": TARGET, "text": "I can search videos."}]


class FakeSlackFailingOnSecondPost(FakeSlack):
    def post(self, channel, text):
        if len(self.posted) == 1:
            raise SlackError("chat.postMessage refused: ratelimited")
        return super().post(channel, text)


class FakeSlackNoChannelInfo(FakeSlack):
    def channel_info(self, channel):
        raise AssertionError("should not have called Slack for a non-listed channel")

    def members(self, channel):
        raise AssertionError("should not have called Slack for a non-listed channel")

    def replies(self, channel, ts):
        raise AssertionError("should not have called Slack for a non-listed channel")


class FakeSources:
    def find(self, channel, hint):
        return [BundleSource(RepoRef("acme", "agents", "main"), "a" * 40, "bundles/assets",
                             {".claude-plugin/plugin.json": '{"name": "asset-search"}'})]


class FakeSourcesEmpty:
    def find(self, channel, hint):
        return []


class FakeSourcesMultiple:
    def find(self, channel, hint):
        return [
            BundleSource(RepoRef("acme", "agents", "main"), "a" * 40, "bundles/video",
                         {".claude-plugin/plugin.json": '{"name": "asset-search"}'}),
            BundleSource(RepoRef("acme", "agents", "main"), "b" * 40, "bundles/style-guide",
                         {".claude-plugin/plugin.json": '{"name": "style-guide"}'}),
        ]


class FakeSourcesByName:
    """One bundle per repository, selected by the name hint."""

    BUNDLES = {
        "in-agents": RepoRef("acme", "agents", "main"),
        "in-tools": RepoRef("acme", "tools", "main"),
        "in-unlisted": RepoRef("evil", "elsewhere", "main"),
    }

    def find(self, channel, hint):
        repo = self.BUNDLES[hint]
        return [BundleSource(repo, "c" * 40, hint,
                             {".claude-plugin/plugin.json": json.dumps({"name": hint})})]


class RefuseEverySource:
    def find(self, channel, hint):
        raise AssertionError("should not have read Git for a non-listed channel")


class FakeIssues:
    def __init__(self):
        self.created = []
        self.searched = []

    def find_open(self, repo, query):
        self.searched.append((repo.full_name, query))
        return []

    def create(self, repo, title, body):
        self.created.append((repo.full_name, title, body))
        return f"https://github.com/{repo.full_name}/issues/1"


class FakeClock:
    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def call(server, name, args):
    async def go():
        entry = server._lowlevel_server.get_request_handler("tools/call")
        return await entry.handler(None, mcp_types.CallToolRequestParams(name=name, arguments=args))
    return anyio.run(go)


def text(result):
    return result.content[0].text


def send(server, probes, target=TARGET, channel="C0EXAMPLE2"):
    args = {"channel": channel, "target_user": target, "probes": probes}
    return call(server, "send_probes", args)


def test_the_tool_surface_is_exactly_five_tools():
    server = build(CONFIG, FakeSlack(), FakeSources())

    async def go():
        return await server.list_tools()
    assert sorted(t.name for t in anyio.run(go)) == [
        "collect_replies", "file_issue", "find_open_issue", "read_target", "send_probes",
    ]


def test_the_issue_tools_take_the_destination_as_an_argument_the_card_shows():
    server = build(CONFIG, FakeSlack(), FakeSources())

    async def go():
        return {t.name: t for t in await server.list_tools()}
    tools = anyio.run(go)
    for name in ("file_issue", "find_open_issue"):
        assert "repository" in tools[name].input_schema["required"], name


def test_file_issue_refuses_before_a_target_was_read():
    server = build(CONFIG, FakeSlack(), FakeSources(), issues=FakeIssues())
    refused = call(server, "file_issue", {"repository": "acme/agents", "title": "t", "body": "b"})
    assert refused.is_error is True and "read_target" in text(refused)


def test_find_open_issue_refuses_before_a_target_was_read():
    server = build(CONFIG, FakeSlack(), FakeSources(), issues=FakeIssues())
    refused = call(server, "find_open_issue", {"repository": "acme/agents", "query": "q"})
    assert refused.is_error is True and "read_target" in text(refused)


def test_the_issue_tools_refuse_a_repository_read_target_never_returned():
    issues = FakeIssues()
    server = build(TWO_REPOS, FakeSlack(), FakeSourcesByName(), issues=issues)
    read = call(server, "read_target", {
        "channel": "C0EXAMPLE2", "target_user": TARGET, "bundle_name": "in-agents",
    })
    assert read.is_error is False
    # acme/tools is listed in MEAN_TESTER_REPOS, but no target was read from it.
    filed = call(server, "file_issue", {"repository": "acme/tools", "title": "t", "body": "b"})
    searched = call(server, "find_open_issue", {"repository": "acme/tools", "query": "q"})
    assert filed.is_error is True and "read_target" in text(filed)
    assert searched.is_error is True and "read_target" in text(searched)
    assert issues.created == [] and issues.searched == []


def test_two_targets_read_in_sequence_each_file_only_to_their_own_repository():
    issues = FakeIssues()
    server = build(TWO_REPOS, FakeSlack(), FakeSourcesByName(), issues=issues)
    for name in ("in-agents", "in-tools"):
        read = call(server, "read_target", {
            "channel": "C0EXAMPLE2", "target_user": TARGET, "bundle_name": name,
        })
        assert read.is_error is False
    # Reading the second target does not move the first one's filings.
    for repository in ("acme/agents", "acme/tools"):
        filed = call(server, "file_issue", {"repository": repository, "title": "t", "body": "b"})
        assert filed.is_error is False, text(filed)
        assert json.loads(text(filed))["url"].startswith(f"https://github.com/{repository}/")
    assert [c[0] for c in issues.created] == ["acme/agents", "acme/tools"]


def test_a_target_from_a_repository_outside_the_listed_ones_is_never_filed_to():
    issues = FakeIssues()
    server = build(TWO_REPOS, FakeSlack(), FakeSourcesByName(), issues=issues)
    read = call(server, "read_target", {
        "channel": "C0EXAMPLE2", "target_user": TARGET, "bundle_name": "in-unlisted",
    })
    assert read.is_error is False
    filed = call(server, "file_issue", {"repository": "evil/elsewhere", "title": "t", "body": "b"})
    assert filed.is_error is True and "MEAN_TESTER_REPOS" in text(filed)
    assert issues.created == []


def test_find_open_issue_refuses_clearly_in_replay_mode_after_a_target_was_read():
    server = build(CONFIG, FakeSlack(), FakeSources(), issues=None)
    read = call(server, "read_target", {"channel": "C0EXAMPLE2", "target_user": TARGET})
    assert read.is_error is False
    refused = call(server, "find_open_issue", {"repository": "acme/agents", "query": "q"})
    assert refused.is_error is True
    assert "replay mode" in text(refused)
    assert "report the FAIL without filing" in text(refused)


def test_file_issue_refuses_clearly_in_replay_mode_after_a_target_was_read():
    server = build(CONFIG, FakeSlack(), FakeSources(), issues=None)
    read = call(server, "read_target", {"channel": "C0EXAMPLE2", "target_user": TARGET})
    assert read.is_error is False
    refused = call(server, "file_issue", {"repository": "acme/agents", "title": "t", "body": "b"})
    assert refused.is_error is True
    assert "replay mode" in text(refused)
    assert "report the FAIL without filing" in text(refused)


def test_a_round_posts_marked_probes_and_reads_final_replies():
    slack = FakeSlack()
    server = build(CONFIG, slack, FakeSources())
    sent = send(server, ["what can you search?"])
    assert sent.is_error is False
    assert slack.posted == ["[mean test] <@U0TARGET01> what can you search?"]
    got = call(server, "collect_replies", {
        "channel": "C0EXAMPLE2", "target_user": TARGET, "probe_ts": ["1790000000.000101"],
    })
    assert got.is_error is False
    assert "I can search videos." in text(got)


def test_a_guard_refusal_is_an_error_on_the_wire():
    server = build(CONFIG, FakeSlack(), FakeSources())
    refused = send(server, ["hi"], channel="C0EXAMPLE8")
    assert refused.is_error is True
    assert "not an operator-listed channel" in text(refused)


def test_read_target_reports_the_bundle_and_whether_the_target_is_in_channel():
    slack = FakeSlack()
    server = build(CONFIG, slack, FakeSources())

    present = call(server, "read_target", {"channel": "C0EXAMPLE2", "target_user": TARGET})
    assert present.is_error is False
    body = json.loads(text(present))
    assert body == {
        "bundle": "asset-search",
        "repository": "acme/agents",
        "commit": "a" * 40,
        "path": "bundles/assets",
        "files": {".claude-plugin/plugin.json": '{"name": "asset-search"}'},
        "spec": {},
        "spec_omitted": [],
        "target_in_channel": True,
    }

    absent = call(server, "read_target", {"channel": "C0EXAMPLE2", "target_user": "U0ELSEWHERE"})
    assert absent.is_error is False
    assert json.loads(text(absent))["target_in_channel"] is False


def test_read_target_refuses_an_unlisted_channel_without_reading_slack_or_git():
    server = build(CONFIG, FakeSlackNoChannelInfo(), RefuseEverySource())
    refused = call(server, "read_target", {"channel": "C0EXAMPLE8", "target_user": TARGET})
    assert refused.is_error is True
    assert "not an operator-listed channel" in text(refused)


def test_read_target_refuses_when_no_bundle_matches():
    server = build(CONFIG, FakeSlack(), FakeSourcesEmpty())
    refused = call(server, "read_target", {"channel": "C0EXAMPLE2", "target_user": TARGET})
    assert refused.is_error is True
    assert "no bundle" in text(refused)


def test_read_target_refuses_and_lists_candidates_when_several_bundles_match():
    server = build(CONFIG, FakeSlack(), FakeSourcesMultiple())
    refused = call(server, "read_target", {"channel": "C0EXAMPLE2", "target_user": TARGET})
    assert refused.is_error is True
    assert "asset-search" in text(refused)
    assert "style-guide" in text(refused)
    assert "bundle_name" in text(refused)


def test_send_probes_refuses_an_unlisted_channel_without_calling_slack():
    server = build(CONFIG, FakeSlackNoChannelInfo(), FakeSources())
    refused = send(server, ["hi"], channel="C0EXAMPLE8")
    assert refused.is_error is True
    assert "not an operator-listed channel" in text(refused)


def test_send_probes_refuses_a_target_who_is_not_in_the_channel():
    slack = FakeSlack(members=("U0SOMEONE1",))
    server = build(CONFIG, slack, FakeSources())
    refused = send(server, ["hi"])
    assert refused.is_error is True
    assert "not a member" in text(refused)
    assert slack.posted == []


def test_a_failed_post_reports_the_probes_already_posted_so_a_retry_skips_them():
    slack = FakeSlackFailingOnSecondPost()
    server = build(CONFIG, slack, FakeSources())
    refused = send(server, ["first", "second", "third"])
    assert refused.is_error is True
    message = text(refused)
    assert "probe 2 of 3" in message and "ratelimited" in message
    assert "1790000000.000101" in message
    assert "[mean test] <@U0TARGET01> first" in message
    # The probe that did post can still be collected.
    got = call(server, "collect_replies", {
        "channel": "C0EXAMPLE2", "target_user": TARGET, "probe_ts": ["1790000000.000101"],
    })
    assert got.is_error is False


def test_collect_replies_refuses_an_unlisted_channel_without_calling_slack():
    server = build(CONFIG, FakeSlackNoChannelInfo(), FakeSources())
    refused = call(server, "collect_replies", {
        "channel": "C0EXAMPLE8", "target_user": TARGET, "probe_ts": ["1790000000.000101"],
    })
    assert refused.is_error is True
    assert "not an operator-listed channel" in text(refused)


def test_collect_replies_refuses_a_ts_this_connector_never_posted():
    server = build(TWO_REPOS, FakeSlack(), FakeSources())
    never = call(server, "collect_replies", {
        "channel": "C0EXAMPLE2", "target_user": TARGET, "probe_ts": ["1790000000.999999"],
    })
    assert never.is_error is True and "send_probes" in text(never)

    assert send(server, ["hi"]).is_error is False
    # The same ts in another listed channel is some other message, not our probe.
    elsewhere = call(server, "collect_replies", {
        "channel": "C0EXAMPLE3", "target_user": TARGET, "probe_ts": ["1790000000.000101"],
    })
    assert elsewhere.is_error is True and "send_probes" in text(elsewhere)


def test_the_probe_cap_holds_across_calls_within_the_window():
    clock = FakeClock()
    server = build(WINDOWED, FakeSlack(), FakeSources(), now=clock)
    assert send(server, ["a", "b", "c"]).is_error is False
    over = send(server, ["d", "e"])
    assert over.is_error is True
    assert "at most 4" in text(over) and "240 s" in text(over)
    assert send(server, ["d"]).is_error is False
    clock.t += 100
    assert "140 s" in text(send(server, ["e"]))
    clock.t += 140
    assert send(server, ["e", "f", "g", "h"]).is_error is False


def test_distinct_targets_per_channel_are_capped_within_the_window():
    clock = FakeClock()
    members = ("U0TARGET01", "U0TARGET02", "U0TARGET03")
    server = build(WINDOWED, FakeSlack(members=members), FakeSources(), now=clock)
    assert WINDOWED.max_concurrent_rounds == 2
    assert send(server, ["a"], target="U0TARGET01").is_error is False
    clock.t += 10
    assert send(server, ["a"], target="U0TARGET02").is_error is False
    third = send(server, ["a"], target="U0TARGET03")
    assert third.is_error is True
    assert "2 targets" in text(third) and "230 s" in text(third)
    # A target already being probed may continue its own round, which keeps
    # it active for another window.
    assert send(server, ["b"], target="U0TARGET01").is_error is False
    clock.t += 241
    assert send(server, ["a"], target="U0TARGET03").is_error is False


def test_read_target_returns_the_bundles_spec():
    class WithSpec:
        def find(self, channel, hint):
            return [BundleSource(RepoRef("acme", "agents", "main"), "a" * 40, "bundles/assets",
                                 {".claude-plugin/plugin.json": '{"name": "asset-search"}'},
                                 spec={"docs/spec.md": "# Spec"}, spec_omitted=("docs/big.md",))]
    server = build(CONFIG, FakeSlack(), WithSpec())
    body = json.loads(text(call(server, "read_target", {
        "channel": "C0EXAMPLE2", "target_user": TARGET,
    })))
    assert body["spec"] == {"docs/spec.md": "# Spec"}
    assert body["spec_omitted"] == ["docs/big.md"]


class GatedSlack(FakeSlack):
    """Holds every post until the test opens the gate, so tool calls overlap."""

    def __init__(self, members=(TARGET,)):
        super().__init__(members)
        self.gate = threading.Event()
        self.changed = threading.Condition()
        self.entered = 0

    def post(self, channel, text):
        with self.changed:
            self.entered += 1
            self.changed.notify_all()
        assert self.gate.wait(timeout=5), "the test never opened the gate"
        with self.changed:
            return super().post(channel, text)


def overlapping(slack, calls):
    """Run each call on its own thread, starting the next only once the previous
    one is either held inside `post` or finished, then open the gate."""
    results = [None] * len(calls)
    threads = []
    for i, fn in enumerate(calls):
        done = threading.Event()

        def run(i=i, fn=fn, done=done):
            try:
                results[i] = fn()
            finally:
                with slack.changed:
                    done.set()
                    slack.changed.notify_all()

        with slack.changed:
            before = slack.entered
        thread = threading.Thread(target=run)
        thread.start()
        threads.append(thread)
        with slack.changed:
            assert slack.changed.wait_for(
                lambda done=done, before=before: done.is_set() or slack.entered > before,
                timeout=5,
            )
    slack.gate.set()
    for thread in threads:
        thread.join(timeout=5)
        assert not thread.is_alive()
    return results


def test_two_concurrent_full_rounds_to_one_target_post_at_most_one_round():
    slack = GatedSlack()
    server = build(WINDOWED, slack, FakeSources(), now=FakeClock())
    round_ = ["a", "b", "c", "d"]
    results = overlapping(slack, [lambda: send(server, round_), lambda: send(server, round_)])
    assert len(slack.posted) <= 4
    assert sorted(r.is_error for r in results) == [False, True]
    refused = next(r for r in results if r.is_error)
    assert "at most 4" in text(refused)


def test_concurrent_probes_to_distinct_targets_stay_within_the_channel_cap():
    members = ("U0TARGET01", "U0TARGET02", "U0TARGET03")
    slack = GatedSlack(members=members)
    server = build(WINDOWED, slack, FakeSources(), now=FakeClock())
    assert WINDOWED.max_concurrent_rounds == 2
    results = overlapping(slack, [lambda u=u: send(server, ["a"], target=u) for u in members])
    assert len(slack.posted) <= 2
    assert sorted(r.is_error for r in results) == [False, False, True]
    refused = next(r for r in results if r.is_error)
    assert "2 targets" in text(refused)


def test_a_failed_post_gives_back_the_slots_of_the_probes_it_never_posted():
    class FailsOnceOnSecondPost(FakeSlack):
        failed = False

        def post(self, channel, text):
            if len(self.posted) == 1 and not self.failed:
                self.failed = True
                raise SlackError("chat.postMessage refused: ratelimited")
            return super().post(channel, text)

    server = build(WINDOWED, FailsOnceOnSecondPost(), FakeSources(), now=FakeClock())
    failed = send(server, ["a", "b", "c", "d"])
    assert failed.is_error is True and "probe 2 of 4" in text(failed)
    # One probe posted, so three of the round's four slots are free again.
    assert send(server, ["b", "c", "d"]).is_error is False
    assert send(server, ["e"]).is_error is True


def test_collect_replies_refuses_a_probe_old_enough_to_have_been_pruned():
    clock = FakeClock()
    server = build(CONFIG, FakeSlack(), FakeSources(), now=clock)
    assert send(server, ["first"]).is_error is False
    # Past twice the reply window, the next send prunes the first probe.
    clock.t += 3 * CONFIG.reply_timeout_s
    assert send(server, ["second"]).is_error is False
    pruned = call(server, "collect_replies", {
        "channel": "C0EXAMPLE2", "target_user": TARGET, "probe_ts": ["1790000000.000101"],
    })
    assert pruned.is_error is True and "send_probes" in text(pruned)
    kept = call(server, "collect_replies", {
        "channel": "C0EXAMPLE2", "target_user": TARGET, "probe_ts": ["1790000000.000102"],
    })
    assert kept.is_error is False
