import json

import pytest
from mean_tester_probes.config import Config, ConfigError, RepoRef

CREDENTIALS = json.dumps({"slack_bot_token": "xoxb-test", "github_token": "ghp_test"})

BASE = {
    "MEAN_TESTER_CREDENTIALS": CREDENTIALS,
    "MEAN_TESTER_CHANNELS": "C0EXAMPLE2, C0EXAMPLE3",
    "MEAN_TESTER_REPOS": "curie-eng/curie@next, acme/agents@main",
}


def test_reads_channels_and_repos_from_env():
    config = Config.from_env(BASE)
    assert config.channels == frozenset({"C0EXAMPLE2", "C0EXAMPLE3"})
    assert config.repos == (
        RepoRef("curie-eng", "curie", "next"), RepoRef("acme", "agents", "main"),
    )
    assert config.max_probes == 4
    assert config.slack_token == "xoxb-test"
    assert config.github_token == "ghp_test"


def test_names_every_missing_variable_at_once():
    with pytest.raises(ConfigError) as err:
        Config.from_env({})
    for name in ("MEAN_TESTER_CREDENTIALS", "MEAN_TESTER_CHANNELS", "MEAN_TESTER_REPOS"):
        assert name in str(err.value)


def test_a_round_can_never_be_configured_above_four_probes():
    # ADR 0169 d4: at most four probes per round. The cap is a ceiling the
    # operator can lower, never raise.
    with pytest.raises(ConfigError, match="MEAN_TESTER_MAX_PROBES"):
        Config.from_env({**BASE, "MEAN_TESTER_MAX_PROBES": "5"})
    assert Config.from_env({**BASE, "MEAN_TESTER_MAX_PROBES": "2"}).max_probes == 2


def test_a_repo_without_a_ref_is_refused():
    with pytest.raises(ConfigError, match="owner/name@ref"):
        Config.from_env({**BASE, "MEAN_TESTER_REPOS": "curie-eng/curie"})


def test_invalid_json_credentials_are_refused():
    with pytest.raises(ConfigError, match="MEAN_TESTER_CREDENTIALS"):
        Config.from_env({**BASE, "MEAN_TESTER_CREDENTIALS": "not json"})


def test_a_missing_github_token_names_it():
    creds = json.dumps({"slack_bot_token": "xoxb-test"})
    with pytest.raises(ConfigError, match="github_token") as err:
        Config.from_env({**BASE, "MEAN_TESTER_CREDENTIALS": creds})
    assert "MEAN_TESTER_CREDENTIALS" in str(err.value)


def test_a_config_error_never_carries_a_token_value():
    creds = json.dumps({"slack_bot_token": "xoxb-super-secret-value"})
    with pytest.raises(ConfigError) as err:
        Config.from_env({**BASE, "MEAN_TESTER_CREDENTIALS": creds})
    assert "xoxb-super-secret-value" not in str(err.value)


def test_concurrent_rounds_default_to_two_and_stay_within_one_to_four():
    assert Config.from_env(BASE).max_concurrent_rounds == 2
    raised = Config.from_env({**BASE, "MEAN_TESTER_MAX_CONCURRENT_ROUNDS": "4"})
    assert raised.max_concurrent_rounds == 4
    for bad in ("0", "5"):
        with pytest.raises(ConfigError, match="MEAN_TESTER_MAX_CONCURRENT_ROUNDS"):
            Config.from_env({**BASE, "MEAN_TESTER_MAX_CONCURRENT_ROUNDS": bad})


def test_spec_paths_are_an_optional_json_object_of_relative_directories():
    assert Config.from_env(BASE).spec_paths == {}
    paths = json.dumps({"asset-search": "docs/specs/assets/"})
    assert Config.from_env({**BASE, "MEAN_TESTER_SPEC_PATHS": paths}).spec_paths == {
        "asset-search": "docs/specs/assets",
    }
    for bad in ("not json", "[]", '{"a": 1}', '{"a": "/abs"}', '{"a": "../up"}', '{"a": ""}'):
        with pytest.raises(ConfigError, match="MEAN_TESTER_SPEC_PATHS"):
            Config.from_env({**BASE, "MEAN_TESTER_SPEC_PATHS": bad})
