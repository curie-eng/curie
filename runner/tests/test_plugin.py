"""Plugin bundle loading and validation against the frozen plugin-format."""

from pathlib import Path

import pytest
from curie_runner import PluginBundleError, load_plugins

_FIXTURES = Path(__file__).resolve().parents[2] / "packages/plugin-format/tests/fixtures"


def test_no_plugin_dir_is_empty() -> None:
    assert load_plugins(None) == []
    assert load_plugins("") == []


def test_valid_bundle_becomes_local_plugin_config() -> None:
    bundle = _FIXTURES / "valid_bundle"
    plugins = load_plugins(str(bundle))
    assert plugins == [{"type": "local", "path": str(bundle)}]


def test_invalid_bundle_raises() -> None:
    bundle = _FIXTURES / "bad_manifest_name"
    with pytest.raises(PluginBundleError):
        load_plugins(str(bundle))


def test_bundle_system_prompt_read_from_manifest(tmp_path: Path) -> None:
    """The manifest ``systemPrompt`` is read from the bundle (epic #30, #271)."""
    from curie_runner import load_bundle_system_prompt

    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin" / "plugin.json").write_text(
        '{"name": "demo", "systemPrompt": "Be terse and cite the CRM."}',
        encoding="utf-8",
    )
    assert load_bundle_system_prompt(str(tmp_path)) == "Be terse and cite the CRM."


def test_bundle_system_prompt_absent_is_none(tmp_path: Path) -> None:
    from curie_runner import load_bundle_system_prompt

    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin" / "plugin.json").write_text(
        '{"name": "demo"}', encoding="utf-8"
    )
    assert load_bundle_system_prompt(str(tmp_path)) is None
    # No plugin dir, and a dir with no manifest, both resolve to None.
    assert load_bundle_system_prompt(None) is None
    assert load_bundle_system_prompt("") is None


def test_bundle_system_prompt_bad_manifest_is_none(tmp_path: Path) -> None:
    """A malformed manifest is non-fatal here (load_plugins is the real gate)."""
    from curie_runner import load_bundle_system_prompt

    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin" / "plugin.json").write_text("{ not json", encoding="utf-8")
    assert load_bundle_system_prompt(str(tmp_path)) is None


def test_bundle_web_search_defaults_on_and_accepts_explicit_boolean(tmp_path: Path) -> None:
    from curie_runner import load_bundle_web_search_enabled

    assert load_bundle_web_search_enabled(None) is True
    assert load_bundle_web_search_enabled(str(tmp_path)) is True

    config = tmp_path / "curie.bundle.json"
    config.write_text('{"webSearch": true}', encoding="utf-8")
    assert load_bundle_web_search_enabled(str(tmp_path)) is True

    config.write_text('{"webSearch": false}', encoding="utf-8")
    assert load_bundle_web_search_enabled(str(tmp_path)) is False


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ("not json", "expected JSON object"),
        ("[]", "root must be a JSON object"),
        ('{"websearch": false}', "unknown key"),
        ('{"webSearch": "false"}', "must be a JSON boolean"),
    ],
)
def test_bundle_web_search_invalid_config_fails_closed(
    tmp_path: Path, body: str, message: str
) -> None:
    from curie_runner import load_bundle_web_search_enabled

    (tmp_path / "curie.bundle.json").write_text(body, encoding="utf-8")
    with pytest.raises(PluginBundleError, match=message):
        load_bundle_web_search_enabled(str(tmp_path))


# --- #2612: a claim whose staging env never reached the init containers -------
#
# The silent no-op this pins: a hand-written SandboxClaim carrying
# CURIE_BUNDLE_REF in spec.env with no containerName reaches the runner only.
# bundle-fetch and bundle-extract keep the SandboxTemplate's empty default, take
# their no-op path, exit 0, and the runner boots with the ref set over an empty
# plugin dir. Before #2612 that surfaced as the frozen validator's
# [manifest.missing], which blames the bundle. These tests fail if the
# diagnosis regresses to that silent mis-attribution.


def test_set_bundle_ref_with_nothing_staged_names_the_claim_shape(tmp_path: Path) -> None:
    from curie_runner.plugin import BUNDLE_INIT_CONTAINERS, BUNDLE_REF_ENV

    with pytest.raises(PluginBundleError) as excinfo:
        load_plugins(str(tmp_path), env={BUNDLE_REF_ENV: "bundles/agent-v7.tgz"})

    message = str(excinfo.value)
    # The cause, not the symptom: the claim shape is named, every init container
    # that needs targeting is named, and the misleading frozen-validator code is
    # NOT what the operator is handed.
    assert "containerName" in message
    assert "spec.env" in message
    for container in BUNDLE_INIT_CONTAINERS:
        assert container in message
    assert "manifest.missing" not in message


def test_unset_bundle_ref_with_nothing_staged_stays_the_bundle_error(tmp_path: Path) -> None:
    """No ref means no staging was ever asked for; the old error is correct."""
    from curie_runner.plugin import BUNDLE_REF_ENV

    with pytest.raises(PluginBundleError) as excinfo:
        load_plugins(str(tmp_path), env={})
    assert "containerName" not in str(excinfo.value)

    # An empty or whitespace-only ref is the template's baked default, not an
    # operator intent, and must not trip the diagnosis either.
    with pytest.raises(PluginBundleError) as excinfo:
        load_plugins(str(tmp_path), env={BUNDLE_REF_ENV: "   "})
    assert "containerName" not in str(excinfo.value)


def test_set_bundle_ref_with_a_staged_bundle_loads_normally() -> None:
    """Staging worked: the ref is set AND a manifest is present. No diagnosis."""
    from curie_runner.plugin import BUNDLE_REF_ENV

    bundle = _FIXTURES / "valid_bundle"
    plugins = load_plugins(str(bundle), env={BUNDLE_REF_ENV: "bundles/agent-v7.tgz"})
    assert plugins == [{"type": "local", "path": str(bundle)}]


def test_set_bundle_ref_with_a_staged_but_malformed_bundle_keeps_the_bundle_error() -> None:
    """A manifest that exists but is wrong is a bundle fault, not a claim fault."""
    from curie_runner.plugin import BUNDLE_REF_ENV

    bundle = _FIXTURES / "bad_manifest_name"
    with pytest.raises(PluginBundleError) as excinfo:
        load_plugins(str(bundle), env={BUNDLE_REF_ENV: "bundles/agent-v7.tgz"})
    assert "containerName" not in str(excinfo.value)


def test_env_defaults_to_the_process_environment(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The boot path calls load_plugins with no env; os.environ must be the source."""
    from curie_runner.plugin import BUNDLE_REF_ENV

    monkeypatch.setenv(BUNDLE_REF_ENV, "bundles/agent-v7.tgz")
    with pytest.raises(PluginBundleError, match="containerName"):
        load_plugins(str(tmp_path))


def test_a_bundle_that_did_extract_but_ships_no_manifest_is_still_a_bundle_fault(
    tmp_path: Path,
) -> None:
    """Files present means staging ran; the frozen validator's verdict is correct.

    The emptiness test is what keeps the #2612 diagnosis from claiming every
    manifest-less bundle is a mis-shaped claim.
    """
    from curie_runner.plugin import BUNDLE_REF_ENV

    (tmp_path / "skills").mkdir()
    with pytest.raises(PluginBundleError) as excinfo:
        load_plugins(str(tmp_path), env={BUNDLE_REF_ENV: "bundles/agent-v7.tgz"})
    assert "containerName" not in str(excinfo.value)


def test_a_missing_plugin_dir_with_a_set_ref_is_diagnosed(tmp_path: Path) -> None:
    """The init pair creates the dir; no dir at all is the same never-staged case."""
    from curie_runner.plugin import BUNDLE_REF_ENV

    with pytest.raises(PluginBundleError, match="containerName"):
        load_plugins(
            str(tmp_path / "absent"), env={BUNDLE_REF_ENV: "bundles/agent-v7.tgz"}
        )
