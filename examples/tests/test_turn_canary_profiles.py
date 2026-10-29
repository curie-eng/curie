"""Executable synthetic profile boundaries. @spec TURN-CANARY-16"""

import asyncio
import copy
import json

import pytest
from aci_protocol.turn import QueuedTurn
from channel_protocol import ReplyUpdate
from channel_protocol.progress import ProgressCard

from examples.tests.test_turn_canary import (
    REPLY_REF,
    CyclePlatform,
    _binding,
    _canary,
    reply_events,
    target_for,
)

FAST = {"turn_deadline": 0.02, "cleanup_deadline": 0.02, "poll_period": 0.001}
ROUTE = "slack|C-EXAMPLE-1|blue"
PROMPT = "Describe the synthetic read-only observation in one sentence."


def _fields(**overrides):
    """A deliberately generic text contract. @spec TURN-CANARY-16"""
    return {
        "prompt": PROMPT,
        "required_any": [["observation", "summary"], ["ready"]],
        "forbidden_any": ["cannot comply", "approval required"],
        **overrides,
    }


def _profile(canary, **overrides):
    """Construct the public programmatic contract. @spec TURN-CANARY-16"""
    fields = _fields(**overrides)
    return canary.ProbeProfile(
        prompt=fields["prompt"],
        required_any=tuple(tuple(group) for group in fields["required_any"]),
        forbidden_any=tuple(fields["forbidden_any"]),
    )


def _env(raw=None):
    """Minimum isolated configuration without real endpoints. @spec TURN-CANARY-16"""
    env = {
        "TURN_CANARY_DATABASE_URL": "postgresql+asyncpg://probe@db.example.invalid/curie",
        "TURN_CANARY_VALKEY_URL": "redis://cache.example.invalid:6379/0",
        "TURN_CANARY_API_URL": "http://api.example.invalid",
        "TURN_CANARY_API_KEY": "synthetic-key",
        "TURN_CANARY_STREAM": "curie:runs",
        "TURN_CANARY_STATE_PATH": "/tmp/synthetic-turn-canary/state",
        "TURN_CANARY_DISCOVER": "1",
    }
    if raw is not None:
        env["TURN_CANARY_PROBE_PROFILES"] = raw
    return env


def _turn(canary, profile=None):
    """Build one correlated synthetic turn. @spec TURN-CANARY-16"""
    return canary.build_turn(
        _binding(),
        nonce="nonce-1",
        conversation_id="eval:profile-example",
        reply_ref=REPLY_REF,
        event_id="EvCANARY-profile-example",
        received_at="2026-01-01T00:00:00Z",
        profile=profile,
    )


class ProfilePlatform(CyclePlatform):
    """Own-turn-only in-memory runtime with explicit final text. @spec TURN-CANARY-16"""

    def __init__(self, text="observation ready", agent="sample-agent", failure=None, **kwargs):
        """@spec TURN-CANARY-16"""
        super().__init__(**kwargs)
        self.text = text
        self.agent = agent
        self.failure = failure

    async def inventory(self):
        """@spec TURN-CANARY-16"""
        return [{**_binding(), "agent": self.agent}]

    async def enqueue(self, turn):
        """@spec TURN-CANARY-16"""
        if self.failure == "enqueue":
            raise RuntimeError("synthetic enqueue failure")
        await super().enqueue(turn)

    async def replies(self, reply_ref, after):
        """@spec TURN-CANARY-16"""
        self.calls.append(("replies", after))
        if self.failure == "relay":
            raise RuntimeError("synthetic relay failure")
        if self.failure == "timeout":
            return {"events": [], "next_cursor": after, "terminal": False}
        text = self.text
        if text == "__nonce__":
            text = self.turn.text.split("no other text: ", 1)[1].split(". Do not", 1)[0]
        events = reply_events(self.turn, text)
        return {"events": events, "next_cursor": len(events), "terminal": True}


def _cycle(canary, platform, **kwargs):
    """Select exactly the anonymous route under test. @spec TURN-CANARY-16"""
    return asyncio.run(
        canary.run_cycle(platform, [("slack", "C-EXAMPLE-1", "blue")], **FAST, **kwargs)
    )


def _samples(text, name):
    """Read only sample lines for one metric family. @spec TURN-CANARY-16"""
    return [line for line in text.splitlines() if line.startswith(name + "{")]


def test_profile_config_is_opt_in_and_materializes_immutable_profile_values():
    """@spec TURN-CANARY-16"""
    canary = _canary()
    assert canary.parse_config(_env()).probe_profiles == {}
    parsed = canary.parse_config(_env(json.dumps({"sample-agent": _fields()})))
    profile = parsed.probe_profiles["sample-agent"]
    assert profile.prompt == PROMPT
    assert profile.required_any == (("observation", "summary"), ("ready",))
    assert profile.forbidden_any == ("cannot comply", "approval required")


@pytest.mark.parametrize(
    "raw",
    [
        "",
        " ",
        "{}",
        "[]",
        "null",
        "{",
        '{"sample-agent":{}}',
        json.dumps({"sample-agent": _fields(extra="unsupported")}),
        json.dumps({"sample-agent": {"prompt": PROMPT, "required_any": [["ready"]]}}),
        '{"sample-agent":'
        + json.dumps(_fields())
        + ',"sample-agent":'
        + json.dumps(_fields())
        + "}",
        '{"sample-agent":{"prompt":"one","prompt":"two",'
        '"required_any":[["ready"]],"forbidden_any":[]}}',
    ],
)
def test_config_rejects_empty_malformed_duplicate_and_unknown_profiles(raw):
    """@spec TURN-CANARY-16"""
    with pytest.raises(ValueError):
        _canary().parse_config(_env(raw))


@pytest.mark.parametrize("slug", ["", "Sample", "-sample", "sample_name", "é", "a" * 64])
def test_config_requires_bounded_exact_ascii_inventory_slugs(slug):
    """@spec TURN-CANARY-16"""
    with pytest.raises(ValueError):
        _canary().parse_config(_env(json.dumps({slug: _fields()})))


@pytest.mark.parametrize(
    "change",
    [
        {"prompt": " "},
        {"prompt": 1},
        {"prompt": "p" * 2049},
        {"required_any": []},
        {"required_any": [[]]},
        {"required_any": [["m"]] * 9},
        {"required_any": [["m"] * 9]},
        {"required_any": ["ready"]},
        {"required_any": [[True]]},
        {"required_any": [[""]]},
        {"required_any": [["\u3000\t"]]},
        {"required_any": [["m" * 129]]},
        {"forbidden_any": "refusal"},
        {"forbidden_any": [None]},
        {"forbidden_any": [" "]},
        {"forbidden_any": ["m" * 129]},
        {"forbidden_any": ["m"] * 33},
    ],
)
def test_profile_field_bounds_are_checked_through_config(change):
    """@spec TURN-CANARY-16"""
    with pytest.raises(ValueError):
        _canary().parse_config(_env(json.dumps({"sample-agent": _fields(**change)})))


@pytest.mark.parametrize(
    "change",
    [
        {"prompt": ""},
        {"prompt": "p" * 2049},
        {"required_any": ()},
        {"required_any": ((),)},
        {"required_any": (("m",),) * 9},
        {"required_any": (("m",) * 9,)},
        {"required_any": (("\u3000",),)},
        {"required_any": (("m" * 129,),)},
        {"forbidden_any": (" ",)},
        {"forbidden_any": ("m" * 129,)},
        {"forbidden_any": ("m",) * 33},
    ],
)
def test_programmatic_profiles_enforce_the_same_bounds(change):
    """@spec TURN-CANARY-16"""
    canary = _canary()
    values = {"prompt": PROMPT, "required_any": (("ready",),), "forbidden_any": (), **change}
    with pytest.raises(ValueError):
        canary.ProbeProfile(**values)


def test_config_accepts_maximum_nested_bounds_and_limits_profile_count_and_utf8_bytes():
    """@spec TURN-CANARY-16"""
    canary = _canary()
    edge = _fields(
        prompt="p" * 2048, required_any=[["m" * 128] * 8] * 8, forbidden_any=["f" * 128] * 32
    )
    assert canary.parse_config(_env(json.dumps({"a" * 63: edge}))).probe_profiles
    hundred = {f"sample-{index}": _fields() for index in range(100)}
    assert len(canary.parse_config(_env(json.dumps(hundred))).probe_profiles) == 100
    with pytest.raises(ValueError):
        canary.parse_config(_env(json.dumps({**hundred, "extra": _fields()})))
    # Non-ASCII strings ensure a byte bound cannot accidentally become a character bound.
    large = json.dumps(
        {f"sample-{index}": _fields(prompt="界" * 2048) for index in range(11)}, ensure_ascii=False
    )
    assert len(large) < 65536 < len(large.encode("utf-8"))
    with pytest.raises(ValueError):
        canary.parse_config(_env(large))


def test_profile_prompt_retains_no_tools_suffix_and_protected_wire():
    """@spec TURN-CANARY-16"""
    canary = _canary()
    default = _turn(canary)
    profiled = _turn(canary, _profile(canary))
    suffix = default.text[default.text.index(". Do not") :]
    assert profiled.text.startswith(PROMPT) and profiled.text.endswith(suffix)
    wire = QueuedTurn.model_validate_json(profiled.model_dump_json())
    assert wire.tool_access.value == "read-only"
    assert wire.attachments == [] and wire.hook_run is None
    assert wire.reply_handle.adapter == "curie-cluster-message"
    assert wire.reply_handle.identity == "blue"
    assert wire.reply_handle.placeholder == REPLY_REF
    assert wire.conversation_id.startswith("eval:")


@pytest.mark.parametrize(
    ("text", "matches"),
    [
        ("ＯＢＳＥＲＶＡＴＩＯＮ\n\tＲＥＡＤＹ", True),
        ("A summary is ready", True),
        ("observation pending", False),
        ("ready", False),
        ("observation ready but CANNOT\nCOMPLY", False),
        ("observation ready; approval required", False),
    ],
)
def test_profile_requires_every_alternative_group_and_excludes_normalized_forbidden(text, matches):
    """@spec TURN-CANARY-16"""
    canary = _canary()
    turn = _turn(canary)
    observed = canary.assess_relay(
        reply_events(turn, text), nonce="nonce-1", turn=turn, profile=_profile(canary)
    )
    assert observed.probe_success is matches
    assert observed.probe_reason == ("profile_match" if matches else "profile_mismatch")
    assert observed.delivery_success is True
    assert observed.exact_nonce is False and observed.reason == "nonce_mismatch"


def test_both_marker_and_reply_normalize_and_metacharacters_stay_literal():
    """@spec TURN-CANARY-16"""
    canary = _canary()
    turn = _turn(canary)
    profile = _profile(
        canary,
        required_any=[["Ａ\t Ｂ", "Straße"], ["a.*[b]"]],
        forbidden_any=["ＮＯ\n ＴＨＡＮＫＳ"],
    )
    for text, expected in [
        ("a b a.*[b]", True),
        ("STRASSE a.*[b]", True),
        ("a b axxxb", False),
        ("a b a.*[b] no thanks", False),
    ]:
        assert profile.matches(text) is expected
        result = canary.assess_relay(
            reply_events(turn, text), nonce="nonce-1", turn=turn, profile=profile
        )
        assert result.probe_success is expected


def test_profile_assessment_requires_the_enqueued_turn_for_correlation():
    """@spec TURN-CANARY-16"""
    canary = _canary()
    turn = _turn(canary)
    result = canary.assess_relay(
        reply_events(turn, "observation ready"), nonce="nonce-1", profile=_profile(canary)
    )
    assert result.probe_success is False
    assert result.probe_reason == "profile_mismatch"


@pytest.mark.parametrize(
    "case",
    [
        "missing",
        "blank",
        "incomplete",
        "not_delivered",
        "wrong_target",
        "malformed",
        "progress_only",
        "last_text_mismatch",
        "last_text_blank",
    ],
)
def test_profile_cannot_bypass_delivery_or_use_progress_or_stale_text(case):
    """@spec TURN-CANARY-16"""
    canary = _canary()
    turn = _turn(canary)
    events = reply_events(turn, "observation ready")
    if case == "missing":
        events = events[-1:]
    elif case == "blank":
        events[0]["text"] = " \n"
    elif case == "incomplete":
        events.pop()
    elif case == "not_delivered":
        events[-1]["outcome"] = "dropped"
    elif case == "wrong_target":
        events[0]["target"]["address"] = "C-OTHER"
    elif case == "malformed":
        events[0]["text"] = 123
    elif case == "progress_only":
        events[0] = ReplyUpdate(
            version="1.1",
            event="reply.update",
            target=target_for(turn),
            delivery_id="00000000-0000-4000-8000-000000000002",
            progress=ProgressCard(
                kind="card",
                state="complete",
                summary="observation ready",
                revision=1,
                terminal=True,
            ),
        ).model_dump(mode="json")
    else:
        latest = copy.deepcopy(events[0])
        latest["text"] = " " if case == "last_text_blank" else "unrelated"
        events.insert(1, latest)
    result = canary.assess_relay(events, nonce="nonce-1", turn=turn, profile=_profile(canary))
    assert result.probe_success is False and result.probe_reason == "profile_mismatch"


def test_default_assessment_probe_tracks_legacy_nonce_without_profile_reason():
    """@spec TURN-CANARY-16"""
    canary = _canary()
    turn = _turn(canary)
    for text, expected, reason in [
        ("nonce-1", True, "exact_nonce"),
        ("observation ready", False, "nonce_mismatch"),
    ]:
        result = canary.assess_relay(reply_events(turn, text), nonce="nonce-1", turn=turn)
        assert result.probe_success is result.exact_nonce is expected
        assert result.probe_reason is None and result.reason == reason


@pytest.mark.parametrize(("text", "matched"), [("observation ready", True), ("unrelated", False)])
def test_profile_cycle_reports_independent_probe_delivery_and_nonce_results(text, matched):
    """@spec TURN-CANARY-16"""
    canary = _canary()
    platform = ProfilePlatform(text=text)
    result = _cycle(canary, platform, probe_profiles={"sample-agent": _profile(canary)})
    assert platform.turn.text.startswith(PROMPT)
    assert result.target_probe_success == {ROUTE: matched}
    assert result.target_probe_profile == {ROUTE: True}
    assert result.target_delivery_success == {ROUTE: True}
    assert result.target_last_delivery_success[ROUTE] > 0
    assert bool(result.target_last_probe_success) is matched
    assert result.target_success == {ROUTE: False}
    assert result.success is matched
    assert bool(result.last_success) is matched
    assert result.target_last_success == {}
    assert {"phase": "relay", "outcome": "nonce_mismatch"} in result.logs
    assert {
        "phase": "probe",
        "outcome": "profile_match" if matched else "profile_mismatch",
    } in result.logs
    assert platform.active == 0
    assert (
        len([call for call in platform.calls if isinstance(call, tuple) and call[0] == "reset"])
        == 1
    )
    exposed = repr(result.logs) + canary.prometheus_text(result)
    assert (
        PROMPT not in exposed
        and "cannot comply" not in exposed
        and "observation ready" not in exposed
    )
    split = canary.prometheus_text(result, labels="split")
    assert (
        'turn_canary_target_probe_profile{agent="sample-agent",kind="slack",'
        'address="C-EXAMPLE-1",adapter="blue"} 1' in split
    )


@pytest.mark.parametrize("agent", ["sample-agent-extra", "Sample-agent", ""])
def test_profile_selection_is_exact_and_unmatched_agents_keep_nonce_contract(agent):
    """@spec TURN-CANARY-16"""
    canary = _canary()
    platform = ProfilePlatform(text="__nonce__", agent=agent)
    result = _cycle(canary, platform, probe_profiles={"sample-agent": _profile(canary)})
    assert "no other text: " in platform.turn.text
    assert result.target_probe_profile == {ROUTE: False}
    assert result.target_probe_success == result.target_success == {ROUTE: True}
    assert result.success is True
    assert result.target_last_probe_success[ROUTE] > 0
    assert result.target_last_delivery_success[ROUTE] > 0
    assert result.target_last_success[ROUTE] > 0 and result.last_success > 0


@pytest.mark.parametrize("failure", ["enqueue", "relay", "timeout"])
def test_attempt_failures_publish_probe_failure_and_no_success_timestamps(failure):
    """@spec TURN-CANARY-16"""
    canary = _canary()
    result = _cycle(
        canary, ProfilePlatform(failure=failure), probe_profiles={"sample-agent": _profile(canary)}
    )
    assert result.target_probe_success == {ROUTE: False}
    assert result.target_probe_profile == {ROUTE: True}
    assert result.target_last_delivery_success == result.target_last_probe_success == {}
    assert result.success is False


def test_capacity_and_cleanup_holds_publish_no_profile_samples_or_restart_timestamps(tmp_path):
    """@spec TURN-CANARY-16"""
    canary = _canary()
    profiles = {"sample-agent": _profile(canary)}
    skipped = _cycle(canary, ProfilePlatform(headroom=False), probe_profiles=profiles)
    assert skipped.target_probe_success == skipped.target_probe_profile == {}
    state = tmp_path / "state.json"
    with canary.StateJournal(state) as journal:
        journal.data["cleanup_blocked"] = True
        successful = canary.CycleResult(
            target_last_delivery_success={ROUTE: 10.0}, target_last_probe_success={ROUTE: 10.0}
        )
        journal.save_result(successful)
        held = _cycle(canary, ProfilePlatform(), journal=journal, probe_profiles=profiles)
        assert held.target_probe_success == held.target_probe_profile == {}
    with canary.StateJournal(state) as journal:
        restored = _cycle(canary, ProfilePlatform(), journal=journal, probe_profiles=profiles)
    assert restored.target_last_delivery_success == restored.target_last_probe_success == {}


def test_exporter_retains_profile_observations_and_timestamps_until_inventory_removes_route():
    """@spec TURN-CANARY-16"""
    canary = _canary()
    limits = canary.Limits(
        cycle_interval=1,
        turn_deadline=0.02,
        cleanup_deadline=0.02,
        poll_period=0.001,
        max_targets=1,
    )
    monitor = canary.Monitor(None, limits)
    names = [
        "target_probe_success",
        "target_probe_profile",
        "target_last_delivery_success_timestamp_seconds",
        "target_last_probe_success_timestamp_seconds",
    ]
    assert all(not _samples(monitor.metrics_text(), "turn_canary_" + name) for name in names)
    other = "slack|C-EXAMPLE-2|blue"
    monitor.observe(
        canary.CycleResult(
            target_probe_success={ROUTE: True},
            target_probe_profile={ROUTE: True},
            target_last_delivery_success={ROUTE: 10.0},
            target_last_probe_success={ROUTE: 10.0},
            inventory_labels={ROUTE, other},
        )
    )
    monitor.observe(
        canary.CycleResult(
            target_probe_success={other: False},
            target_probe_profile={other: False},
            inventory_labels={ROUTE, other},
        )
    )
    monitor.observe(canary.CycleResult(inventory_labels=None))
    before_failure = monitor.metrics_text()
    assert all(
        any("C-EXAMPLE-1" in row for row in _samples(before_failure, "turn_canary_" + name))
        for name in names
    )
    monitor.observe(
        canary.CycleResult(
            target_probe_success={ROUTE: False},
            target_probe_profile={ROUTE: True},
            inventory_labels={ROUTE, other},
        )
    )
    failed = monitor.metrics_text()
    assert any(
        row.endswith("} 0") and "C-EXAMPLE-1" in row
        for row in _samples(failed, "turn_canary_target_probe_success")
    )
    for name in names[2:]:
        assert _samples(failed, "turn_canary_" + name) == _samples(
            before_failure, "turn_canary_" + name
        )
    monitor.observe(canary.CycleResult(inventory_labels={other}))
    assert "C-EXAMPLE-1" not in monitor.metrics_text()
    assert "C-EXAMPLE-2" in monitor.metrics_text()


@pytest.mark.parametrize(
    "bad_mapping",
    ["not-a-mapping", {"Bad-Slug": None}, {"sample-agent": {"prompt": "ready"}}, "too-many"],
)
def test_direct_cycle_rejects_invalid_profile_mapping_before_platform_io(bad_mapping):
    """@spec TURN-CANARY-16"""
    canary = _canary()
    if bad_mapping == "too-many":
        bad_mapping = {f"sample-{index}": _profile(canary) for index in range(101)}

    class NoIO:
        """Reject every attempted runtime boundary access. @spec TURN-CANARY-16"""

        def __getattr__(self, name):
            """@spec TURN-CANARY-16"""
            pytest.fail(f"platform access preceded profile validation: {name}")

    with pytest.raises(ValueError):
        _cycle(canary, NoIO(), probe_profiles=bad_mapping)


@pytest.mark.parametrize("case", ["malformed", "wrong_target", "incomplete"])
def test_polling_matching_text_still_requires_valid_correlated_completion(case):
    """@spec TURN-CANARY-16"""
    canary = _canary()

    class Corrupt(ProfilePlatform):
        """Corrupt only the external reply boundary. @spec TURN-CANARY-16"""

        async def replies(self, reply_ref, after):
            """@spec TURN-CANARY-16"""
            page = await super().replies(reply_ref, after)
            if case == "malformed":
                page["events"][-1]["version"] = "unsupported"
            elif case == "wrong_target":
                page["events"][0]["target"]["address"] = "C-UNRELATED"
            else:
                page["events"].pop()
                page["next_cursor"] = 1
                page["terminal"] = False
            return page

    platform = Corrupt()
    result = _cycle(canary, platform, probe_profiles={"sample-agent": _profile(canary)})
    assert result.target_probe_success == {ROUTE: False}
    assert result.target_delivery_success == {ROUTE: False}
    assert result.target_last_delivery_success == result.target_last_probe_success == {}
    assert platform.active == 0


@pytest.mark.parametrize("standing", [False, True])
def test_main_carries_environment_profile_into_real_cycle_and_standing_monitor(
    tmp_path, monkeypatch, standing
):
    """Replace only external installation services. @spec TURN-CANARY-16"""
    canary = _canary()
    installed = {}
    signals = []
    env = _env(json.dumps({"sample-agent": _fields()}))
    env.update(
        {
            "TURN_CANARY_STATE_PATH": str(tmp_path / "state.json"),
            "TURN_CANARY_METRICS_PATH": str(tmp_path / "metrics.txt"),
            "TURN_CANARY_TURN_DEADLINE": ".02",
            "TURN_CANARY_CLEANUP_DEADLINE": ".02",
            "TURN_CANARY_POLL_PERIOD": ".001",
        }
    )
    if standing:
        env["TURN_CANARY_LISTEN"] = "127.0.0.1:9464"

    class InstalledPlatform(ProfilePlatform):
        """A synthetic installation boundary with no live clients. @spec TURN-CANARY-16"""

        def __init__(self, **settings):
            """@spec TURN-CANARY-16"""
            super().__init__()
            installed["platform"] = self
            self.closed = False

        async def reset_state(self, agent_id, thread_key):
            """Stop standing mode once the real cycle cleans up. @spec TURN-CANARY-16"""
            result = await super().reset_state(agent_id, thread_key)
            if standing:
                signals[0]()
            return result

        async def close(self):
            """@spec TURN-CANARY-16"""
            self.closed = True

    class InstalledServer:
        """No socket is required to test monitor wiring. @spec TURN-CANARY-16"""

        def close(self):
            """@spec TURN-CANARY-16"""
            installed["server_closed"] = True

        async def wait_closed(self):
            """@spec TURN-CANARY-16"""
            installed["server_joined"] = True

    async def install_http(monitor, host, port):
        """Retain the real monitor and replace its listener only. @spec TURN-CANARY-16"""
        assert (host, port) == ("127.0.0.1", 9464)
        installed["monitor"] = monitor
        return InstalledServer()

    def install_signal(signum, callback):
        """Replace OS signal registration, retaining its callback. @spec TURN-CANARY-16"""
        signals.append(callback)

    async def scenario():
        """Run the real entry point with bounded installation lifetime. @spec TURN-CANARY-16"""
        monkeypatch.setattr(asyncio.get_running_loop(), "add_signal_handler", install_signal)
        return await asyncio.wait_for(canary.main(), 1)

    monkeypatch.setattr(canary.os, "environ", env)
    monkeypatch.setattr(canary, "RuntimePlatform", InstalledPlatform)
    monkeypatch.setattr(canary, "serve_http", install_http)
    exit_code = asyncio.run(scenario())
    platform = installed["platform"]
    assert platform.turn.text.startswith(PROMPT)
    assert platform.closed and platform.active == 0
    if standing:
        assert exit_code == 0
        assert installed["server_closed"] and installed["server_joined"]
        metrics = installed["monitor"].metrics_text()
    else:
        # The configured probe qualifies the cycle independently of its nonce.
        assert exit_code == 0
        metrics = (tmp_path / "metrics.txt").read_text()
    assert _samples(metrics, "turn_canary_target_probe_success")[0].endswith("} 1")
    assert _samples(metrics, "turn_canary_target_probe_profile")[0].endswith("} 1")
    assert _samples(metrics, "turn_canary_target_success")[0].endswith("} 0")
