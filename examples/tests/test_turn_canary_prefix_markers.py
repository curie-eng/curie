"""Optional literal response-prefix exclusions. @spec TURN-CANARY-18"""

import asyncio
import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

from examples.tests.test_turn_canary import _canary
from examples.tests.test_turn_canary_profiles import ProfilePlatform, _env, _samples

HELPER = Path(__file__).resolve().parents[1] / "turn-canary" / "probe_profiles.py"
PROMPT = "Describe the synthetic status and available capabilities without tools."


def _fields(**overrides):
    """Generic status contract with independent marker groups. @spec TURN-CANARY-18"""
    return {
        "prompt": PROMPT,
        "required_any": [["status", "summary"], ["ready"]],
        "forbidden_any": ["blocked permanently"],
        **overrides,
    }


def _profile(canary, prefixes=(), **overrides):
    """Exercise the public constructor through the runtime re-export. @spec TURN-CANARY-18"""
    return canary.ProbeProfile(**_fields(forbidden_prefix_any=prefixes, **overrides))


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("Unable to provide status ready", False),
        ("\n\tＵＮＡＢＬＥ\u3000 ＴＯ provide status ready", False),
        ("Status ready; unable to perform write operations.", True),
        ("Summary ready. No capability changes were requested.", True),
        ("NO CAPABILITY changes; status ready", False),
        ("Status ready; all capabilities remain read-only.", True),
    ],
)
def test_prefix_exclusions_match_normalized_start_and_allow_same_wording_later(text, expected):
    """@spec TURN-CANARY-18"""
    profile = _profile(_canary(), prefixes=["ＵＮＡＢＬＥ\n ＴＯ", "no capability"])
    assert profile.matches(text) is expected


def test_prefixes_are_literal_and_required_and_anywhere_forbidden_markers_still_apply():
    """@spec TURN-CANARY-18"""
    profile = _profile(_canary(), prefixes=["a.*[b]"])
    assert not profile.matches("a.*[b] status ready")
    assert profile.matches("axxxb status ready")
    assert profile.matches("status ready; a.*[b]")
    assert not profile.matches("status ready; blocked permanently")
    assert not profile.matches("available capabilities ready")
    assert not profile.matches("status pending")


def test_legacy_constructor_and_json_and_explicit_empty_prefix_remain_compatible():
    """@spec TURN-CANARY-18"""
    canary = _canary()
    old = canary.ProbeProfile(PROMPT, (("status",), ("ready",)), ("blocked permanently",))
    parsed = canary.parse_probe_profiles(json.dumps({"sample-agent": _fields()}))["sample-agent"]
    explicit = _profile(canary, prefixes=[])
    parsed_empty = canary.parse_probe_profiles(
        json.dumps({"sample-agent": _fields(forbidden_prefix_any=[])})
    )["sample-agent"]
    assert (
        old.forbidden_prefix_any
        == parsed.forbidden_prefix_any
        == explicit.forbidden_prefix_any
        == ()
    )
    assert parsed_empty.forbidden_prefix_any == ()
    for profile in (old, parsed, explicit, parsed_empty):
        assert profile.matches("Unable to act; status ready")
        assert not profile.matches("status ready; blocked permanently")


@pytest.mark.parametrize("construction", ["programmatic", "json"])
@pytest.mark.parametrize(
    "bad",
    [
        None,
        "unable to",
        {},
        [["unable to"]],
        [False],
        [""],
        ["\u3000\n\t"],
        ["x" * 129],
        ["x"] * 33,
    ],
)
def test_invalid_prefix_collections_and_markers_fail_in_both_entry_points(construction, bad):
    """@spec TURN-CANARY-18"""
    canary = _canary()
    with pytest.raises(ValueError):
        if construction == "programmatic":
            _profile(canary, prefixes=bad)
        else:
            canary.parse_probe_profiles(
                json.dumps(
                    {
                        "sample-agent": _fields(forbidden_prefix_any=bad),
                    }
                )
            )


def test_maximum_prefix_bounds_canonicalize_and_detach_mutable_input():
    """@spec TURN-CANARY-18"""
    canary = _canary()
    markers = ["p" * 128] * 32
    programmatic = _profile(canary, prefixes=markers)
    parsed = canary.parse_probe_profiles(
        json.dumps(
            {
                "sample-agent": _fields(forbidden_prefix_any=markers),
            }
        )
    )["sample-agent"]
    markers[0] = "changed"
    assert programmatic.forbidden_prefix_any == parsed.forbidden_prefix_any == ("p" * 128,) * 32
    assert isinstance(programmatic.forbidden_prefix_any, tuple)
    assert programmatic.matches("status ready")


def test_optional_field_does_not_relax_unknown_duplicate_or_required_key_validation():
    """@spec TURN-CANARY-18"""
    canary = _canary()
    value = _fields(forbidden_prefix_any=["unable to"])
    invalid = {**value, "unexpected": []}
    missing = {key: field for key, field in value.items() if key != "forbidden_any"}
    duplicate = '{"sample-agent":' + json.dumps(value)[:-1] + ',"forbidden_prefix_any":[]}}'
    for raw in (
        json.dumps({"sample-agent": invalid}),
        json.dumps({"sample-agent": missing}),
        duplicate,
    ):
        with pytest.raises(ValueError):
            canary.parse_probe_profiles(raw)
    assert canary.parse_probe_profiles(json.dumps({"sample-agent": value}))


def test_prefix_markers_count_toward_total_utf8_json_byte_limit():
    """@spec TURN-CANARY-18"""
    raw = json.dumps(
        {f"sample-{index}": _fields(forbidden_prefix_any=["界" * 128] * 32) for index in range(6)},
        ensure_ascii=False,
    )
    assert len(raw) < 65536 < len(raw.encode("utf-8"))
    with pytest.raises(ValueError):
        _canary().parse_probe_profiles(raw)


def test_copied_standalone_helper_matches_prefixes_without_site_packages(tmp_path):
    """Load the real schema alone under isolated stdlib Python. @spec TURN-CANARY-18"""
    copied = tmp_path / "probe_profiles.py"
    shutil.copyfile(HELPER, copied)
    program = '''
# @spec TURN-CANARY-18
import importlib.util
import sys

def block_runtime_dependencies(event, args):
    """Reject runtime dependency import attempts. @spec TURN-CANARY-18"""
    if event == 'import' and args[0].split('.', 1)[0] in {
        'aci_protocol', 'channel_protocol', 'pydantic',
    }:
        raise AssertionError('runtime dependency attempted')

sys.addaudithook(block_runtime_dependencies)
before = list(sys.path)
spec = importlib.util.spec_from_file_location('standalone_prefix_contract', sys.argv[1])
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
profile = module.parse_probe_profiles(sys.argv[2])['sample-agent']
assert profile.forbidden_prefix_any == ('Unable to',)
assert not profile.matches('  ＵＮＡＢＬＥ\u3000ＴＯ provide status ready')
assert profile.matches('Status ready; unable to take actions')
assert not profile.matches('Status ready; blocked permanently')
assert sys.path == before
assert not {'aci_protocol', 'channel_protocol', 'pydantic'} & sys.modules.keys()
print('standalone-prefix-ok')
'''
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-c",
            program,
            str(copied),
            json.dumps({"sample-agent": _fields(forbidden_prefix_any=["Unable to"])}),
        ],
        cwd=tmp_path,
        env={},
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert result.stdout.strip() == "standalone-prefix-ok"


@pytest.mark.parametrize(
    ("text", "matches"),
    [
        ("Unable to act; status ready", False),
        ("Status ready; unable to perform writes", True),
    ],
)
def test_main_configuration_and_cycle_use_prefix_contract_without_changing_nonce_or_delivery(
    tmp_path, monkeypatch, capsys, text, matches
):
    """Replace only the external platform; run real main/config/cycle. @spec TURN-CANARY-18"""
    canary = _canary()
    installed = []
    env = _env(json.dumps({"sample-agent": _fields(forbidden_prefix_any=["Unable to"])}))
    metrics = tmp_path / "metrics.txt"
    env.update(
        {
            "TURN_CANARY_STATE_PATH": str(tmp_path / "state.json"),
            "TURN_CANARY_METRICS_PATH": str(metrics),
            "TURN_CANARY_TURN_DEADLINE": ".02",
            "TURN_CANARY_CLEANUP_DEADLINE": ".02",
            "TURN_CANARY_POLL_PERIOD": ".001",
        }
    )

    class InstalledPlatform(ProfilePlatform):
        """External boundary with synthetic final text and owned cleanup. @spec TURN-CANARY-18"""

        def __init__(self, **settings):
            """@spec TURN-CANARY-18"""
            super().__init__(text=text)
            installed.append(self)
            self.closed = False

        async def close(self):
            """@spec TURN-CANARY-18"""
            self.closed = True

    monkeypatch.setattr(canary.os, "environ", env)
    monkeypatch.setattr(canary, "RuntimePlatform", InstalledPlatform)
    exit_code = asyncio.run(canary.main())
    assert exit_code == 1
    assert installed[0].turn.text.startswith(PROMPT)
    assert installed[0].closed and installed[0].active == 0
    output = metrics.read_text()
    assert _samples(output, "turn_canary_target_probe_success")[0].endswith(f"}} {int(matches)}")
    assert _samples(output, "turn_canary_target_delivery_success")[0].endswith("} 1")
    assert _samples(output, "turn_canary_target_success")[0].endswith("} 0")
    assert _samples(output, "turn_canary_target_probe_profile")[0].endswith("} 1")
    assert (
        bool(_samples(output, "turn_canary_target_last_probe_success_timestamp_seconds")) is matches
    )
    assert _samples(output, "turn_canary_target_last_delivery_success_timestamp_seconds")
    logs = capsys.readouterr().out
    assert "Unable to" not in logs + output
    assert text not in logs + output
    assert ("profile_match" if matches else "profile_mismatch") in logs
