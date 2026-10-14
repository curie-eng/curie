"""Drive the shipped gate, including refused marked-action evidence.

Slack fields follow https://docs.slack.dev/reference/methods/conversations.replies/
and the production dispatcher card
consumer in approval_actions.py. These are external observations supplied to
the gate, not mocked databases or proof of a live Slack campaign.
"""

import copy
import hashlib
import json
import time

import pytest

from examples.tests.test_mean_tester_gate import (
    CAMPAIGN,
    _base_suite,
    _bound,
    _campaign,
    _import,
    _intake,
    _ok,
    _record_case,
    _suite,
    _token,
    _verdict,
    _write,
)


def test_intake_does_not_echo_internal_suite_bytes(tmp_path):
    _, report = _intake(_suite(tmp_path))
    assert "suite_bytes" not in report


def _fixture(tmp_path):
    data = _base_suite()
    data["cases"][1].update(
        mode="action",
        probe="File the report.",
        card_action="approve",
        expected_state={"report": "filed"},
    )
    suite = _suite(tmp_path, data)
    ledger = tmp_path / "ledger.json"
    evidence = {
        "channel": "C0EXAMPLE1",
        "driver_user": "U0EXAMPLE1",
        "driver_bot": "B0EXAMPLE1",
        "target_user": "U0EXAMPLE2",
        "target_bot": "B0EXAMPLE2",
        "window_seconds": 30,
        "campaign": CAMPAIGN,
        "suite": hashlib.sha256(suite.read_bytes()).hexdigest(),
        "thread": {
            "ok": True,
            "has_more": False,
            "messages": [
                {
                    "ts": "100.000001",
                    "user": "U0EXAMPLE1",
                    "bot_id": "B0EXAMPLE1",
                    "text": "<@U0EXAMPLE2> [test action] ping",
                },
                {
                    "ts": "101.000001",
                    "thread_ts": "100.000001",
                    "user": "U0EXAMPLE2",
                    "bot_id": "B0EXAMPLE2",
                    "text": "This installation accepts test actions from <@U0EXAMPLE1>.",
                },
            ],
        },
        "snapshot": {
            "at": 99,
            "source": "read-own-observation",
            "content": {"report": "absent"},
            "restore_contract": "connector restore",
        },
    }
    evidence = _times(evidence, int(time.time()) - 110)
    return suite, ledger, evidence


def _times(value, offset):
    if isinstance(value, dict):
        return {
            key: (
                str(float(item) + offset)
                if key in {"ts", "thread_ts"}
                else item + offset
                if key == "at"
                else _times(item, offset)
            )
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_times(item, offset) for item in value]
    return value


def _admit(tmp_path, suite, ledger, evidence):
    path = _write(tmp_path / "admission.json", evidence)
    return _bound("admit", suite, ledger, "--evidence", str(path))


def _observation(admission):
    value = {
        "channel": "C0EXAMPLE1",
        "probe": {
            "ts": "102.000001",
            "user": "U0EXAMPLE1",
            "bot_id": "B0EXAMPLE1",
            "text": "<@U0EXAMPLE2> [test action] [mean test a1b2] File the report.",
        },
        "card": {
            "ts": "103.000001",
            "thread_ts": "102.000001",
            "user": "U0EXAMPLE2",
            "bot_id": "B0EXAMPLE2",
            "blocks": [
                {
                    "type": "actions",
                    "elements": [
                        {
                            "type": "button",
                            "action_id": "curie-approval-approve",
                            "value": "acme-approval",
                        },
                        {
                            "type": "button",
                            "action_id": "curie-approval-reject",
                            "value": "acme-approval",
                        },
                    ],
                },
            ],
        },
        "decision": {
            "ts": "104.000001",
            "thread_ts": "102.000001",
            "user": "U0EXAMPLE1",
            "bot_id": "B0EXAMPLE1",
            "text": "<@U0EXAMPLE2> [test action] approve acme-approval",
        },
        "state": {"at": 105, "source": "read-own-observation", "content": {"report": "filed"}},
    }
    return _times(value, int(float(admission["thread"]["messages"][0]["ts"])) - 100)


def _record(tmp_path, suite, ledger, observation, verdict="PASS"):
    path = _write(tmp_path / "observation.json", observation)
    return _record_case(suite, ledger, "c2", 1, verdict, "--evidence", str(path))


def _closeout(admission):
    value = {
        "restoration": {
            "at": 106,
            "source": "read-own-observation",
            "content": {"report": "absent"},
            "cleanup_failures": [],
            "pending_cards": [],
        },
        "configuration": {
            "test": {"testInstallation": True},
            "production": {"testInstallation": False},
            "explanations": {"testInstallation": "Production refuses driver actions."},
        },
        "deployment": {"at": 107, "identity": "acme-prod@commit"},
        "smoke": {
            "at": 109,
            "deployment": "acme-prod@commit",
            "read_only": True,
            "verdict": "PASS",
            "probe": "What reports are available?",
            "reply": "No reports.",
        },
    }
    return _times(value, int(float(admission["thread"]["messages"][0]["ts"])) - 100)


def test_admitted_action_reaches_full_go_only_after_real_closeout(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    _ok(_record(tmp_path, suite, ledger, _observation(evidence)))
    _campaign(suite, ledger, skip_cases=(("c2", 1),))
    pending = _verdict(suite, ledger)
    assert pending.returncode == 1 and "closeout" in pending.stdout
    _ok(
        _bound(
            "closeout",
            suite,
            ledger,
            "--evidence",
            str(_write(tmp_path / "closeout.json", _closeout(evidence))),
        )
    )
    verdict = _ok(_verdict(suite, ledger))
    assert verdict.stdout.startswith("Ship: GO (action scope)")
    restored = tmp_path / "restored.json"
    _ok(_import(suite, restored, _token(verdict)))
    assert _verdict(suite, restored).stdout == verdict.stdout


@pytest.mark.parametrize(
    "damage",
    [
        lambda e: e.update(channel=""),
        lambda e: e.update(campaign="other"),
        lambda e: e.update(suite="0" * 64),
        lambda e: e["thread"].update(has_more=True),
        lambda e: e["thread"].update(ok=False),
        lambda e: e["thread"]["messages"][0].update(thread_ts="90.000001"),
        lambda e: e["thread"]["messages"][0].update(user="U0EXAMPLE3"),
        lambda e: e["thread"]["messages"][0].update(bot_id="B0EXAMPLE3"),
        lambda e: e["thread"]["messages"][1].update(user="U0EXAMPLE3"),
        lambda e: e["thread"]["messages"][1].update(thread_ts="90.000001"),
        lambda e: e["thread"]["messages"][1].update(ts="131.000002"),
        lambda e: e["thread"]["messages"][1].update(
            text="This installation does not accept test actions."
        ),
        lambda e: e["thread"]["messages"].insert(
            1,
            {
                "ts": "100.500000",
                "thread_ts": "100.000001",
                "user": "U0EXAMPLE3",
                "text": "An earlier reply.",
            },
        ),
        lambda e: e["snapshot"].update(content={}),
        lambda e: e["snapshot"].update(restore_contract=""),
    ],
)
def test_invalid_admission_never_unlocks_or_writes_ledger(tmp_path, damage):
    suite, ledger, evidence = _fixture(tmp_path)
    damage(evidence)
    result = _admit(tmp_path, suite, ledger, evidence)
    assert result.returncode == 2 and "refused:" in result.stderr
    assert not ledger.exists()
    assert _record_case(suite, ledger, "c2", 1, "PASS").returncode == 2


@pytest.mark.parametrize(
    "damage",
    [
        lambda e: e.update(channel="C0EXAMPLE2"),
        lambda e: e["probe"].update(text="<@U0EXAMPLE2> File the report."),
        lambda e: e["probe"].update(ts="100.000001"),
        lambda e: e["probe"].update(user="U0EXAMPLE3"),
        lambda e: e["card"].update(user="U0EXAMPLE3"),
        lambda e: e["card"].update(thread_ts="99.000001"),
        lambda e: e["decision"].update(text="<@U0EXAMPLE2> [test action] reject acme-approval"),
        lambda e: e["decision"].update(text="<@U0EXAMPLE2> [test action] approve other-approval"),
        lambda e: e["decision"].update(thread_ts="99.000001"),
        lambda e: e["state"].update(source="target claimed success"),
        lambda e: e["state"].update(content={"report": "absent"}),
        lambda e: e["state"].update(at=103),
    ],
)
def test_wrong_action_or_state_evidence_cannot_record_pass(tmp_path, damage):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    before = ledger.read_bytes()
    observation = _observation(evidence)
    damage(observation)
    result = _record(tmp_path, suite, ledger, observation)
    assert result.returncode == 2 and "refused:" in result.stderr
    assert ledger.read_bytes() == before


@pytest.mark.parametrize(
    "damage",
    [
        lambda e: e["restoration"].update(content={"report": "filed"}),
        lambda e: e["restoration"].update(cleanup_failures=["version conflict"]),
        lambda e: e["restoration"].update(pending_cards=["acme-approval"]),
        lambda e: e["configuration"].update(explanations={}),
        lambda e: e["smoke"].update(at=106),
        lambda e: e["deployment"].update(at=99999999999),
        lambda e: e["smoke"].update(deployment="other-deployment"),
        lambda e: e["smoke"].update(read_only=False),
        lambda e: e["smoke"].update(verdict="UNCLEAR"),
    ],
)
def test_bad_closeout_cannot_turn_passing_action_campaign_green(tmp_path, damage):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    _ok(_record(tmp_path, suite, ledger, _observation(evidence)))
    _campaign(suite, ledger, skip_cases=(("c2", 1),))
    before = ledger.read_bytes()
    closure = copy.deepcopy(_closeout(evidence))
    damage(closure)
    result = _bound(
        "closeout", suite, ledger, "--evidence", str(_write(tmp_path / "closeout.json", closure))
    )
    assert result.returncode == 2 and "refused:" in result.stderr
    assert ledger.read_bytes() == before
    assert _verdict(suite, ledger).returncode == 1


def test_admission_does_not_supply_missing_upload_or_human_click_capability(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    data = json.loads(suite.read_text())
    data["cases"][1]["attachments"] = ["fixtures/report.txt"]
    _write(suite, data)
    evidence["suite"] = hashlib.sha256(suite.read_bytes()).hexdigest()
    _ok(_admit(tmp_path, suite, ledger, evidence))
    assert _record(tmp_path, suite, ledger, _observation(evidence)).returncode == 2
    assert _verdict(suite, ledger).returncode == 1


def test_scenario_only_action_never_gets_readonly_go_without_closeout(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    data = _base_suite()
    _write(suite, data)
    evidence["suite"] = hashlib.sha256(suite.read_bytes()).hexdigest()
    _ok(_admit(tmp_path, suite, ledger, evidence))
    observation = _observation(evidence)
    observation.update(card_action="approve", expected_state={"report": "filed"})
    path = _write(tmp_path / "scenario.json", observation)
    _campaign(suite, ledger, skip_steps=(("onboarding", 1),))
    _ok(
        _bound(
            "record",
            suite,
            ledger,
            "--scenario",
            "onboarding",
            "--step",
            "1",
            "--verdict",
            "PASS",
            "--action",
            "--evidence",
            str(path),
        )
    )
    result = _verdict(suite, ledger)
    assert result.returncode == 1 and "closeout" in result.stdout


def test_blocked_action_scenario_retains_negative_evidence_across_import(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    _ok(_bound("scenario", suite, ledger, "--name", "onboarding", "--steps", "1"))
    path = _write(tmp_path / "scenario.json", _observation(evidence))
    _ok(
        _bound(
            "record",
            suite,
            ledger,
            "--scenario",
            "onboarding",
            "--step",
            "1",
            "--verdict",
            "BLOCKED",
            "--action",
            "--evidence",
            str(path),
        )
    )
    result = _verdict(suite, ledger)
    assert result.returncode == 1 and "onboarding#1" in result.stdout
    imported = tmp_path / "imported.json"
    _ok(_import(suite, imported, _token(result)))
    assert _verdict(suite, imported).stdout == result.stdout


def test_json_boolean_is_not_the_expected_numeric_state(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    data = json.loads(suite.read_text())
    data["cases"][1]["expected_state"] = {"count": 1}
    _write(suite, data)
    evidence["suite"] = hashlib.sha256(suite.read_bytes()).hexdigest()
    _ok(_admit(tmp_path, suite, ledger, evidence))
    observation = _observation(evidence)
    observation["state"]["content"] = {"count": True}
    assert _record(tmp_path, suite, ledger, observation).returncode == 2


def test_closeout_freezes_action_writes_but_does_not_forgive_missing_cases(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    _ok(
        _bound(
            "closeout",
            suite,
            ledger,
            "--evidence",
            str(_write(tmp_path / "closeout.json", _closeout(evidence))),
        )
    )
    assert _record(tmp_path, suite, ledger, _observation(evidence)).returncode == 2
    assert _verdict(suite, ledger).returncode == 1


def test_expired_admission_is_not_reused_to_start_a_new_campaign(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    evidence = _times(evidence, -100)
    result = _admit(tmp_path, suite, ledger, evidence)
    assert result.returncode == 2 and not ledger.exists()


def test_scenario_and_case_names_cannot_overwrite_each_others_action_evidence(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    _ok(_record(tmp_path, suite, ledger, _observation(evidence)))
    _ok(_bound("scenario", suite, ledger, "--name", "c2", "--steps", "1"))
    observation = _observation(evidence)
    path = _write(tmp_path / "scenario.json", observation)
    _ok(
        _bound(
            "record",
            suite,
            ledger,
            "--scenario",
            "c2",
            "--step",
            "1",
            "--verdict",
            "PASS",
            "--action",
            "--evidence",
            str(path),
        )
    )
    assert len(json.loads(ledger.read_text())["actions"]["observations"]) == 2


def test_large_checkpoint_is_no_go_instead_of_silently_losing_evidence(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    # A deterministic poorly compressible source projection, not credential data.
    evidence["snapshot"]["content"]["fingerprints"] = [
        hashlib.sha256(str(i).encode()).hexdigest() for i in range(150)
    ]
    _ok(_admit(tmp_path, suite, ledger, evidence))
    _ok(_record(tmp_path, suite, ledger, _observation(evidence)))
    _campaign(suite, ledger, skip_cases=(("c2", 1),))
    closeout = _closeout(evidence)
    closeout["restoration"]["content"] = copy.deepcopy(evidence["snapshot"]["content"])
    _ok(
        _bound(
            "closeout",
            suite,
            ledger,
            "--evidence",
            str(_write(tmp_path / "closeout.json", closeout)),
        )
    )
    result = _verdict(suite, ledger)
    assert result.returncode == 1 and "checkpoint" in result.stdout
    assert "Ledger:" not in result.stdout


def test_fresh_same_campaign_ping_preserves_prior_case_observations(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    _ok(_record(tmp_path, suite, ledger, _observation(evidence)))
    refreshed = copy.deepcopy(evidence)
    refreshed["thread"] = _times(refreshed["thread"], 5)
    path = _write(tmp_path / "refresh.json", refreshed)
    _ok(_bound("admit", suite, ledger, "--refresh", "--evidence", str(path)))
    assert len(json.loads(ledger.read_text())["actions"]["observations"]) == 1
    assert _verdict(suite, ledger).returncode == 1  # still missing cases and closeout


def test_refreshed_ping_cannot_switch_channel_or_original_baseline(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    before = ledger.read_bytes()
    refreshed = copy.deepcopy(evidence)
    refreshed["thread"] = _times(refreshed["thread"], 5)
    refreshed["snapshot"]["content"] = {"report": "filed"}
    path = _write(tmp_path / "refresh.json", refreshed)
    assert _bound("admit", suite, ledger, "--refresh", "--evidence", str(path)).returncode == 2
    assert ledger.read_bytes() == before


def test_refresh_cannot_change_boolean_snapshot_to_equal_python_integer(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    evidence["snapshot"]["content"]["enabled"] = False
    _ok(_admit(tmp_path, suite, ledger, evidence))
    before = ledger.read_bytes()
    refreshed = copy.deepcopy(evidence)
    refreshed["thread"] = _times(refreshed["thread"], 5)
    refreshed["snapshot"]["content"]["enabled"] = 0
    path = _write(tmp_path / "refresh.json", refreshed)
    assert _bound("admit", suite, ledger, "--refresh", "--evidence", str(path)).returncode == 2
    assert ledger.read_bytes() == before


def test_configuration_cannot_call_a_marked_installation_production(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    _ok(_record(tmp_path, suite, ledger, _observation(evidence)))
    _campaign(suite, ledger, skip_cases=(("c2", 1),))
    closure = _closeout(evidence)
    closure["configuration"]["production"]["testInstallation"] = True
    closure["configuration"]["explanations"] = {}
    path = _write(tmp_path / "closeout.json", closure)
    assert _bound("closeout", suite, ledger, "--evidence", str(path)).returncode == 2
    assert _verdict(suite, ledger).returncode == 1


def test_imported_history_does_not_admit_a_new_probe_outside_its_window(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    # Historical valid admission survives checkpoints as evidence of the past,
    # but cannot qualify a new probe 700 seconds afterward.
    data = json.loads(ledger.read_text())
    data["actions"]["admission"] = _times(data["actions"]["admission"], -700)
    _write(ledger, data)
    before = ledger.read_bytes()
    assert _record(tmp_path, suite, ledger, _observation(evidence)).returncode == 2
    assert ledger.read_bytes() == before


def test_late_state_read_is_not_a_passing_observation(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    data = json.loads(ledger.read_text())
    data["actions"]["admission"] = _times(data["actions"]["admission"], -220)
    _write(ledger, data)
    observation = _times(_observation(evidence), -200)
    observation["state"] = _observation(evidence)["state"]
    assert _record(tmp_path, suite, ledger, observation).returncode == 2


def test_a_corrupt_imported_action_never_recreates_a_green_campaign(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    _ok(_record(tmp_path, suite, ledger, _observation(evidence)))
    data = json.loads(ledger.read_text())
    data["actions"]["observations"]["case:c2#1"]["probe"]["text"] = "Unmarked action."
    _write(ledger, data)
    before = ledger.read_bytes()
    result = _verdict(suite, ledger)
    assert result.returncode == 2 and result.stdout.startswith("Ship: NO-GO")
    assert ledger.read_bytes() == before


@pytest.mark.parametrize(
    "test_value,production_value", [(False, 0), (True, 1), ({"enabled": False}, {"enabled": 0})]
)
def test_every_exact_json_configuration_difference_needs_an_explanation(
    tmp_path, test_value, production_value
):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    closure = _closeout(evidence)
    closure["configuration"]["test"]["limit"] = test_value
    closure["configuration"]["production"]["limit"] = production_value
    path = _write(tmp_path / "closeout.json", closure)
    before = ledger.read_bytes()
    assert _bound("closeout", suite, ledger, "--evidence", str(path)).returncode == 2
    assert ledger.read_bytes() == before


def test_expired_imported_proof_cannot_record_another_old_probe(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    old_evidence = _times(evidence, -700)
    data = json.loads(ledger.read_text())
    data["actions"]["admission"] = old_evidence
    _write(ledger, data)
    token = _token(_verdict(suite, ledger))
    imported = tmp_path / "imported.json"
    _ok(_import(suite, imported, token))
    before = imported.read_bytes()
    assert _record(tmp_path, suite, imported, _observation(old_evidence)).returncode == 2
    assert imported.read_bytes() == before


def test_completed_historical_action_go_does_not_expire_retrospectively(tmp_path):
    suite, ledger, evidence = _fixture(tmp_path)
    _ok(_admit(tmp_path, suite, ledger, evidence))
    _ok(_record(tmp_path, suite, ledger, _observation(evidence)))
    _campaign(suite, ledger, skip_cases=(("c2", 1),))
    _ok(
        _bound(
            "closeout",
            suite,
            ledger,
            "--evidence",
            str(_write(tmp_path / "closeout.json", _closeout(evidence))),
        )
    )
    _write(ledger, _times(json.loads(ledger.read_text()), -700))
    before = _ok(_verdict(suite, ledger))
    imported = tmp_path / "historical.json"
    _ok(_import(suite, imported, _token(before)))
    assert _ok(_verdict(suite, imported)).stdout == before.stdout
