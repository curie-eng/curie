import pytest
from mean_tester_probes.config import Config
from mean_tester_probes.guard import GuardRefusal, ProbeGuard

CONFIG = Config.from_env({
    "MEAN_TESTER_CREDENTIALS": '{"slack_bot_token": "x", "github_token": "y"}',
    "MEAN_TESTER_CHANNELS": "C0EXAMPLE2", "MEAN_TESTER_REPOS": "a/b@main",
})
INTERNAL = {"id": "C0EXAMPLE2", "is_ext_shared": False, "is_shared": False}


def test_marks_and_mentions_every_probe():
    out = ProbeGuard(CONFIG).check("C0EXAMPLE2", INTERNAL, ["what can you search?"], "U0TARGET01")
    assert out == ["[mean test] <@U0TARGET01> what can you search?"]


def test_refuses_a_channel_the_operator_did_not_list():
    with pytest.raises(GuardRefusal, match="not an operator-listed channel"):
        ProbeGuard(CONFIG).check(
            "C0EXAMPLE5", {**INTERNAL, "id": "C0EXAMPLE5"}, ["hi"], "U0TARGET01"
        )


def test_refuses_an_externally_shared_channel_even_when_listed():
    shared = {**INTERNAL, "is_ext_shared": True}
    with pytest.raises(GuardRefusal, match="externally shared"):
        ProbeGuard(CONFIG).check("C0EXAMPLE2", shared, ["hi"], "U0TARGET01")


def test_refuses_more_than_the_round_cap():
    with pytest.raises(GuardRefusal, match="at most 4"):
        ProbeGuard(CONFIG).check("C0EXAMPLE2", INTERNAL, ["a", "b", "c", "d", "e"], "U0TARGET01")


def test_refuses_an_oversized_or_empty_probe():
    with pytest.raises(GuardRefusal, match="empty"):
        ProbeGuard(CONFIG).check("C0EXAMPLE2", INTERNAL, ["  "], "U0TARGET01")
    with pytest.raises(GuardRefusal, match="1500"):
        ProbeGuard(CONFIG).check("C0EXAMPLE2", INTERNAL, ["x" * 1501], "U0TARGET01")


def test_a_probe_cannot_mention_anyone_but_the_target():
    with pytest.raises(GuardRefusal, match="only the target"):
        ProbeGuard(CONFIG).check("C0EXAMPLE2", INTERNAL, ["hey <@U0SOMEONE1>"], "U0TARGET01")


def test_a_channel_info_that_does_not_say_it_is_unshared_is_refused():
    for info in (
        {"id": "C0EXAMPLE2"},
        {"id": "C0EXAMPLE2", "is_shared": False},
        {"id": "C0EXAMPLE2", "is_ext_shared": False},
    ):
        with pytest.raises(GuardRefusal, match="externally shared"):
            ProbeGuard(CONFIG).check("C0EXAMPLE2", info, ["hi"], "U0TARGET01")
