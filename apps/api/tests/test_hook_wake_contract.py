import json
from pathlib import Path

import pytest
from curie_api import bundles, hook_signing, hook_source_signing
from curie_api.hook_partition import HOOK_NAME

CASES = json.loads(
    (Path(__file__).parents[3] / "tests/vectors/hook-wake-declarations.json").read_text()
)["cases"]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["id"])
def test_hook_wake_runtime_declaration_cases(tmp_path: Path, case: dict) -> None:
    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin/plugin.json").write_text(json.dumps(case["manifest"]))
    if not case["declarations_valid"]:
        with pytest.raises(ValueError):
            bundles.read_manifest_wake_triggers(tmp_path)
        return
    declared = bundles.read_manifest_wake_triggers(tmp_path)
    assert list(declared) == case["wake_names"]
    for name, declaration in declared.items():
        assert HOOK_NAME.fullmatch(name)
        assert declaration.name == name
        assert declaration.wake is True
        assert declaration.prompt == next(
            trigger["prompt"]
            for trigger in case["manifest"]["triggers"]
            if trigger.get("name") == name
        )


@pytest.mark.parametrize("contents", [None, "{", "[]", '{"name":"acme-bot","triggers":42}'])
def test_hook_wake_runtime_refuses_unreadable_manifest(
    tmp_path: Path, contents: str | None
) -> None:
    if contents is not None:
        (tmp_path / "plugin.json").write_text(contents)
    with pytest.raises(ValueError):
        bundles.read_manifest_wake_triggers(tmp_path)


def test_hook_wake_runtime_does_not_change_cron_reader(tmp_path: Path) -> None:
    declared = [
        {"type": "cron", "name": "scheduled", "schedule": "0 9 * * *", "prompt": "Read work."}
    ]
    (tmp_path / "plugin.json").write_text(json.dumps({"name": "acme-bot", "triggers": declared}))
    assert bundles.read_manifest_wake_triggers(tmp_path) == {}
    assert bundles.read_manifest_triggers(tmp_path) == declared


def test_hook_wake_runtime_missing_root_has_the_same_refusal(tmp_path: Path) -> None:
    with pytest.raises(ValueError):
        bundles.read_manifest_wake_triggers(tmp_path / "missing")


def test_hook_wake_signature_is_refused_by_existing_ingress_verifiers() -> None:
    corpus = json.loads(
        (Path(__file__).parents[3] / "tests/vectors/hook-wake-signing.json").read_text()
    )
    case = corpus["vectors"][0]
    arguments = {
        "timestamp": case["timestamp"],
        "delivery_id": case["delivery_id"],
        "hook": case["hook"],
        "tool_access": None,
        "body": case["body"].encode(),
        "header": case["expected"]["signature"],
        "now": float(case["timestamp"]),
    }
    key = case["expected"]["derived_key"]
    assert not hook_signing.verify(key, **arguments)
    assert not hook_source_signing.verify_support(key, **arguments)
    for sign, verify in [
        (hook_signing.sign, hook_signing.verify),
        (hook_source_signing.sign_support, hook_source_signing.verify_support),
    ]:
        sign_arguments = {k: v for k, v in arguments.items() if k not in {"header", "now"}}
        assert verify(key, **{**arguments, "header": sign(key, **sign_arguments)})
