from curie_discord_adapter.ingress import DiscordBinding
from curie_discord_adapter.state import DiscordState


def test_continuations_and_completion_dedupe_survive_reopen(tmp_path) -> None:
    path = tmp_path / "discord.sqlite3"
    state = DiscordState(path)
    state.replace_continuations("thread-1", "placeholder-1", ["message-2", "message-3"])
    assert state.mark_completed("event-1") is True
    state.close()

    reopened = DiscordState(path)
    assert reopened.continuations("thread-1", "placeholder-1") == ["message-2", "message-3"]
    assert reopened.mark_completed("event-1") is False
    reopened.close()


def test_delivery_claims_and_thread_routes_are_durable_without_persisting_tokens(tmp_path) -> None:
    path = tmp_path / "discord.sqlite3"
    state = DiscordState(path)
    binding = DiscordBinding(parent_channel_id="111", address="111", token="chn_private")

    assert state.claim_delivery("message-1") is True
    assert state.claim_delivery("message-1") is False
    state.release_delivery("message-1")
    assert state.claim_delivery("message-1") is True
    state.remember_thread("thread-1", binding)
    state.close()

    reopened = DiscordState(path)
    remembered = reopened.thread_binding("thread-1")
    assert remembered is not None
    assert (remembered.parent_channel_id, remembered.address) == ("111", "111")
    assert remembered.token == ""
    assert reopened.claim_delivery("message-1") is False
    reopened.close()
    assert b"chn_private" not in path.read_bytes()
