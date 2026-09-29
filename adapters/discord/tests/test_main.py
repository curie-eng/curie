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


REFUSAL_VECTOR = (
    Path(__file__).resolve().parents[3] / "tests" / "vectors" / "channel-port-refusal.json"
)


def _refusal_response() -> httpx.Response:
    """The platform's caller-list refusal, read from the frozen vector the API
    side is pinned to as well, so the two cannot drift apart."""
    vector = json.loads(REFUSAL_VECTOR.read_text())
    assert set(vector) == {"comment", "status", "detail"}, "unknown key in the frozen vector"
    return httpx.Response(
        int(vector["status"]),
        json={"detail": vector["detail"]},
        request=httpx.Request("POST", "https://curie.example.com/channels/turns"),
    )


class ScriptedHttp:
    def __init__(self, respond) -> None:
        self.respond = respond
        self.calls = 0

    async def post(self, *args, **kwargs) -> httpx.Response:
        self.calls += 1
        return self.respond()

    async def aclose(self) -> None:
        return None


class FakeUser:
    def __init__(self, user_id: int, *, bot: bool = False) -> None:
        self.id = user_id
        self.bot = bot
        self.display_name = f"user-{user_id}"


class FakePosted:
    def __init__(self, message_id: int) -> None:
        self.id = message_id
        self.edits: list[str] = []
        self.deleted = False

    async def edit(self, *, content: str, **kwargs) -> None:
        self.edits.append(content)

    async def delete(self) -> None:
        self.deleted = True


class FakeThread:
    def __init__(self, thread_id: int) -> None:
        self.id = thread_id
        self.sent: list[FakePosted] = []
        self.deleted = False

    async def send(self, text: str, **kwargs) -> FakePosted:
        posted = FakePosted(9000 + len(self.sent))
        self.sent.append(posted)
        return posted

    async def delete(self) -> None:
        self.deleted = True


class FakeChannel:
    def __init__(self, channel_id: int) -> None:
        self.id = channel_id


class FakeMessage:
    def __init__(self, bot_user: FakeUser) -> None:
        self.id = 5555
        self.author = FakeUser(42)
        self.channel = FakeChannel(111)
        self.content = f"<@{bot_user.id}> hello"
        self.mentions = [bot_user]
        self.threads: list[FakeThread] = []

    async def create_thread(self, *, name: str) -> FakeThread:
        thread = FakeThread(7777)
        self.threads.append(thread)
        return thread


def _deliver(tmp_path: Path, respond) -> tuple[FakeMessage, ScriptedHttp, DiscordState]:
    bindings_path = tmp_path / "bindings.json"
    bindings_path.write_text(
        json.dumps([{"parent_channel_id": "111", "address": "111", "token": "chn_ok"}])
    )
    state = DiscordState(tmp_path / "state.sqlite3")
    adapter = DiscordAdapter(config(tmp_path / "state.sqlite3", bindings_path), state)
    http = ScriptedHttp(respond)
    adapter._http = http  # type: ignore[assignment]
    bot_user = FakeUser(1, bot=True)
    adapter._connection.user = bot_user  # type: ignore[assignment]
    message = FakeMessage(bot_user)

    async def scenario() -> None:
        await adapter.on_message(message)  # type: ignore[arg-type]
        # Discord redelivers the same message id on reconnect: a refusal must not repeat.
        await adapter.on_message(message)  # type: ignore[arg-type]
        await adapter.close()

    asyncio.run(scenario())
    return message, http, state


def test_refused_caller_gets_no_thread_placeholder_or_message(tmp_path: Path) -> None:
    message, http, state = _deliver(tmp_path, _refusal_response)

    assert http.calls == 1, "a refusal is final: the delivery is not released for retry"
    assert state.claim_delivery("5555") is False
    [thread] = message.threads
    assert thread.deleted, "the thread created before asking is taken down"
    [placeholder] = thread.sent
    assert placeholder.edits == [], "no error message tells the caller the bot exists"
    state.close()


def test_other_403_still_shows_the_error_and_releases_for_retry(tmp_path: Path) -> None:
    def proxy_403() -> httpx.Response:
        return httpx.Response(
            403,
            json={"detail": "forbidden"},
            request=httpx.Request("POST", "https://curie.example.com/channels/turns"),
        )

    message, http, state = _deliver(tmp_path, proxy_403)

    assert http.calls == 2, "an infrastructure 403 is released and retried"
    assert not any(thread.deleted for thread in message.threads)
    assert message.threads[0].sent[0].edits == [
        "Curie could not accept this message. Please try again."
    ]
    state.close()


def test_api_outage_still_shows_the_error(tmp_path: Path) -> None:
    def outage() -> httpx.Response:
        return httpx.Response(
            503, request=httpx.Request("POST", "https://curie.example.com/channels/turns")
        )

    message, http, state = _deliver(tmp_path, outage)

    assert http.calls == 2
    [thread, _] = message.threads
    assert not thread.deleted
    assert thread.sent[0].edits == ["Curie could not accept this message. Please try again."]
    assert state.claim_delivery("5555") is True
    state.close()
