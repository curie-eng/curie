"""Generic cycle verdicts exercise actual relay assessment and owned cleanup."""

import asyncio

import pytest

from examples.tests.test_turn_canary import _binding, _canary, reply_events
from examples.tests.test_turn_canary_profiles import (
    FAST,
    ROUTE,
    ProfilePlatform,
    _cycle,
    _profile,
)


def test_profile_match_qualifies_cycle_while_nonce_metric_remains_false():
    canary = _canary()
    result = _cycle(canary, ProfilePlatform(), probe_profiles={"sample-agent": _profile(canary)})
    assert result.success is True
    assert result.last_success > 0
    assert result.target_success == {ROUTE: False}
    assert result.target_delivery_success == result.target_probe_success == {ROUTE: True}
    assert result.target_last_success == {}
    assert {"phase": "turn", "outcome": "delivered"} in result.logs
    assert {"phase": "turn", "outcome": "failed"} not in result.logs
    assert "turn_canary_cycle_success 1\n" in canary.prometheus_text(result)


def test_nonce_match_cannot_mask_a_failed_configured_probe(monkeypatch):
    canary = _canary()
    monkeypatch.setattr(canary.secrets, "token_urlsafe", lambda size: "nonce-1")
    result = _cycle(
        canary,
        ProfilePlatform(text="nonce-1"),
        probe_profiles={"sample-agent": _profile(canary)},
    )
    assert result.success is False and result.last_success is None
    assert result.target_success == result.target_delivery_success == {ROUTE: True}
    assert result.target_probe_success == {ROUTE: False}
    assert {"phase": "turn", "outcome": "delivered"} in result.logs
    assert {"phase": "probe", "outcome": "profile_mismatch"} in result.logs


@pytest.mark.parametrize("failure_first", [True, False])
@pytest.mark.parametrize("failure", ["profile", "enqueue", "delivery", "cleanup"])
def test_any_failed_route_keeps_cycle_failed_after_other_routes_pass(failure_first, failure):
    canary = _canary()
    bad_address = "C-EXAMPLE-1" if failure_first else "C-EXAMPLE-2"

    class Routes(ProfilePlatform):
        async def inventory(self):
            return [_binding(), {**_binding(), "address": "C-EXAMPLE-2"}]

        async def enqueue(self, turn):
            if failure == "enqueue" and turn.reply_handle.channel == bad_address:
                self.turn = turn
                raise RuntimeError("synthetic enqueue failure")
            await super().enqueue(turn)

        async def replies(self, reply_ref, after):
            failed = self.turn.reply_handle.channel == bad_address
            text = "unrelated" if failed and failure == "profile" else "observation ready"
            outcome = "dropped" if failed and failure == "delivery" else "delivered"
            events = reply_events(self.turn, text, outcome)
            return {"events": events, "next_cursor": len(events), "terminal": True}

        async def reset_state(self, agent_id, thread_key):
            if failure == "cleanup" and self.turn.reply_handle.channel == bad_address:
                return {"requested": True, "route_existed": True}
            return await super().reset_state(agent_id, thread_key)

    platform = Routes()
    result = asyncio.run(
        canary.run_cycle(
            platform,
            [("slack", "C-EXAMPLE-1", "blue"), ("slack", "C-EXAMPLE-2", "blue")],
            probe_profiles={"sample-agent": _profile(canary)},
            **FAST,
        )
    )
    assert result.success is False and result.last_success is None
    good_address = "C-EXAMPLE-2" if failure_first else "C-EXAMPLE-1"
    assert result.target_probe_success[f"slack|{good_address}|blue"] is True
    assert result.target_delivery_success[f"slack|{good_address}|blue"] is True
    assert (
        len([call for call in platform.calls if isinstance(call, tuple) and call[0] == "reset"])
        == 2
    )
    assert result.cleanup_degraded is (failure == "cleanup")


@pytest.mark.parametrize("failure", ["enqueue", "relay", "timeout"])
def test_profile_configuration_never_qualifies_failed_delivery(failure):
    canary = _canary()
    result = _cycle(
        canary,
        ProfilePlatform(failure=failure),
        probe_profiles={"sample-agent": _profile(canary)},
    )
    assert result.success is False
    assert result.target_delivery_success == result.target_probe_success == {ROUTE: False}


@pytest.mark.parametrize("profile_first", [True, False])
def test_mixed_profile_and_default_routes_each_use_their_selected_contract(profile_first):
    canary = _canary()
    profile_address = "C-EXAMPLE-1" if profile_first else "C-EXAMPLE-2"

    class Mixed(ProfilePlatform):
        async def inventory(self):
            return [
                {
                    **_binding(),
                    "address": address,
                    "agent": "sample-agent" if address == profile_address else "nonce-agent",
                }
                for address in ("C-EXAMPLE-1", "C-EXAMPLE-2")
            ]

        async def replies(self, reply_ref, after):
            if self.turn.reply_handle.channel == profile_address:
                text = "observation ready"
            else:
                text = self.turn.text.split("no other text: ")[1].split(". Do not")[0]
            events = reply_events(self.turn, text)
            return {"events": events, "next_cursor": len(events), "terminal": True}

    platform = Mixed()
    result = asyncio.run(
        canary.run_cycle(
            platform,
            [("slack", "C-EXAMPLE-1", "blue"), ("slack", "C-EXAMPLE-2", "blue")],
            probe_profiles={"sample-agent": _profile(canary)},
            **FAST,
        )
    )
    assert result.success is True and result.last_success > 0
    assert result.target_probe_success == {
        "slack|C-EXAMPLE-1|blue": True,
        "slack|C-EXAMPLE-2|blue": True,
    }
    assert result.target_success[f"slack|{profile_address}|blue"] is False
    assert platform.active == 0 and platform.peak == 1
