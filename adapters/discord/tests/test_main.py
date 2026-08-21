import asyncio
import json
from pathlib import Path

import httpx
import pytest
from curie_discord_adapter.config import DiscordConfig
from curie_discord_adapter.ingress import DiscordBinding
from curie_discord_adapter.main import DiscordAdapter
from curie_discord_adapter.state import DiscordState


class RejectingHttp:
    def __init__(self) -> None:
        self.calls = 0

    async def post(self, *args, **kwargs) -> httpx.Response:
        self.calls += 1
        return httpx.Response(401, request=httpx.Request("POST", "https://curie.example.com"))

    async def aclose(self) -> None:
        return None


def config(path: Path, bindings_path: Path) -> DiscordConfig:
    return DiscordConfig(
        discord_bot_token="bot",
        adapter_secret="reply-secret",
        state_path=path,
        bindings_path=bindings_path,
        curie_api_url="https://curie.example.com",
    )


def test_rotated_binding_file_reenables_a_401_disabled_surface(tmp_path: Path) -> None:
    bindings_path = tmp_path / "bindings.json"
    bindings_path.write_text(
        json.dumps([{"parent_channel_id": "111", "address": "111", "token": "chn_old"}])
    )
    state = DiscordState(tmp_path / "state.sqlite3")
    adapter = DiscordAdapter(config(tmp_path / "state.sqlite3", bindings_path), state)
    old = adapter._binding_for_parent("111")
    assert old is not None
    adapter._disabled_tokens["111"] = old.token
    assert adapter._binding_for_parent("111") is None

    bindings_path.write_text(
        json.dumps([{"parent_channel_id": "111", "address": "111", "token": "chn_new"}])
    )
    rotated = adapter._binding_for_parent("111")
    assert rotated is not None
    assert rotated.token == "chn_new"
    asyncio.run(adapter.close())
    state.close()


def test_curie_401_is_final_and_disables_only_that_token(tmp_path: Path) -> None:
    bindings_path = tmp_path / "bindings.json"
    bindings_path.write_text("[]")
    state = DiscordState(tmp_path / "state.sqlite3")
    adapter = DiscordAdapter(config(tmp_path / "state.sqlite3", bindings_path), state)
    rejecting = RejectingHttp()
    adapter._http = rejecting  # type: ignore[assignment]
    binding = DiscordBinding(parent_channel_id="111", address="111", token="chn_bad")

    with pytest.raises(httpx.HTTPStatusError):
        asyncio.run(adapter._post_turn(binding, {"kind": "discord"}))

    assert rejecting.calls == 1
    assert adapter._disabled_tokens == {"111": "chn_bad"}
    asyncio.run(adapter.close())
    state.close()
