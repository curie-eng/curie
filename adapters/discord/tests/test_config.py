import json
from pathlib import Path

from curie_discord_adapter.config import DiscordConfig


def test_bindings_are_parsed_from_environment_json(monkeypatch) -> None:
    monkeypatch.setenv(
        "CURIE_DISCORD_BINDINGS",
        json.dumps(
            [
                {
                    "parent_channel_id": "111",
                    "address": "111",
                    "token": "chn_example",
                }
            ]
        ),
    )
    config = DiscordConfig(discord_bot_token="bot", adapter_secret="reply-secret")
    assert config.bindings[0].parent_channel_id == "111"
    assert config.bindings[0].token == "chn_example"


def test_bindings_file_is_reread_for_token_rotation(tmp_path: Path) -> None:
    path = tmp_path / "bindings.json"
    path.write_text(
        json.dumps([{"parent_channel_id": "111", "address": "111", "token": "chn_old"}])
    )
    config = DiscordConfig(
        discord_bot_token="bot",
        adapter_secret="reply-secret",
        bindings_path=path,
    )
    assert config.load_bindings()[0].token == "chn_old"

    path.write_text(
        json.dumps([{"parent_channel_id": "111", "address": "111", "token": "chn_new"}])
    )
    assert config.load_bindings()[0].token == "chn_new"
