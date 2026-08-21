"""Run the Discord Gateway client and authenticated reply HTTP server."""

import asyncio
import logging
from typing import Any

import discord
import httpx
import uvicorn

from .config import DiscordConfig
from .egress import DiscordReplyService
from .http import create_reply_app
from .ingress import DiscordBinding, DiscordMessage, build_turn
from .state import DiscordState

logger = logging.getLogger(__name__)


class DiscordAdapter(discord.Client):
    def __init__(self, config: DiscordConfig, state: DiscordState) -> None:
        intents = discord.Intents.none()
        intents.guilds = True
        intents.messages = True
        intents.message_content = True
        super().__init__(intents=intents)
        self._config = config
        self._state = state
        self._disabled_tokens: dict[str, str] = {}
        self._http = httpx.AsyncClient(timeout=20)

    async def close(self) -> None:
        await self._http.aclose()
        await super().close()

    async def on_ready(self) -> None:
        if self.user is not None:
            logger.info("Discord adapter connected as application user %s", self.user.id)

    async def on_message(self, message: discord.Message) -> None:
        if message.author.bot or self.user is None:
            return
        channel = message.channel
        binding: DiscordBinding | None
        require_mention: bool
        thread: discord.Thread | None
        if isinstance(channel, discord.Thread):
            remembered = self._state.thread_binding(str(channel.id))
            if remembered is None:
                return
            binding = self._binding_for_parent(remembered.parent_channel_id)
            if binding is None or binding.address != remembered.address:
                return
            require_mention = False
            thread = channel
        else:
            binding = self._binding_for_parent(str(channel.id))
            if binding is None or self.user not in message.mentions:
                return
            require_mention = True
            thread = None
        preview = self._message(message, str(channel.id))
        if build_turn(
            preview,
            bot_user_id=str(self.user.id),
            binding=binding,
            reply_ref="preview",
            require_mention=require_mention,
        ) is None:
            return
        delivery_id = str(message.id)
        if not self._state.claim_delivery(delivery_id):
            return
        placeholder: discord.Message | None = None
        try:
            if thread is None:
                thread = await message.create_thread(name=self._thread_name(message.content))
                self._state.remember_thread(str(thread.id), binding)
            placeholder = await thread.send(
                self._config.placeholder_text,
                allowed_mentions=discord.AllowedMentions.none(),
            )
            incoming = self._message(message, str(thread.id))
            turn = build_turn(
                incoming,
                bot_user_id=str(self.user.id),
                binding=binding,
                reply_ref=str(placeholder.id),
                require_mention=require_mention,
            )
            if turn is None:
                self._state.release_delivery(delivery_id)
                return
            await self._post_turn(binding, turn)
        except Exception:
            self._state.release_delivery(delivery_id)
            logger.exception("Failed to deliver Discord message %s to Curie", message.id)
            if placeholder is not None:
                await placeholder.edit(
                    content="Curie could not accept this message. Please try again.",
                    allowed_mentions=discord.AllowedMentions.none(),
                )

    def _binding_for_parent(self, parent_channel_id: str) -> DiscordBinding | None:
        bindings = {
            binding.parent_channel_id: binding
            for binding in self._config.load_bindings()
        }
        binding = bindings.get(parent_channel_id)
        if binding is None:
            return None
        disabled_token = self._disabled_tokens.get(binding.address)
        if disabled_token == binding.token:
            return None
        if disabled_token is not None:
            self._disabled_tokens.pop(binding.address, None)
        return binding

    def _message(self, message: discord.Message, thread_id: str) -> DiscordMessage:
        return DiscordMessage(
            id=str(message.id),
            channel_id=str(message.channel.id),
            thread_id=thread_id,
            author_id=str(message.author.id),
            author_name=message.author.display_name,
            content=message.content,
            mentioned_user_ids=frozenset(str(user.id) for user in message.mentions),
        )

    @staticmethod
    def _thread_name(content: str) -> str:
        clean = " ".join(content.split())
        return (clean or "Curie conversation")[:100]

    async def _post_turn(self, binding: DiscordBinding, turn: dict[str, str]) -> None:
        endpoint = f"{self._config.curie_api_url.rstrip('/')}/channels/turns"
        for attempt in range(3):
            try:
                response = await self._http.post(
                    endpoint,
                    headers={"X-API-Key": binding.token},
                    json=turn,
                )
                if response.status_code == 401:
                    self._disabled_tokens[binding.address] = binding.token
                    logger.error(
                        "Curie rejected the scoped token for Discord binding %s; "
                        "ingress for that binding is disabled until the mounted token changes",
                        binding.address,
                    )
                response.raise_for_status()
                return
            except httpx.TransportError:
                if attempt == 2:
                    raise
                await asyncio.sleep(0.25 * (2**attempt))

    async def _channel(self, channel_id: str) -> Any:
        channel = self.get_channel(int(channel_id))
        return channel if channel is not None else await self.fetch_channel(int(channel_id))

    async def edit_message(self, channel_id: str, message_id: str, text: str) -> None:
        channel = await self._channel(channel_id)
        message = await channel.fetch_message(int(message_id))
        await message.edit(content=text, allowed_mentions=discord.AllowedMentions.none())

    async def post_message(self, channel_id: str, text: str) -> str:
        channel = await self._channel(channel_id)
        message = await channel.send(text, allowed_mentions=discord.AllowedMentions.none())
        return str(message.id)

    async def delete_message(self, channel_id: str, message_id: str) -> None:
        channel = await self._channel(channel_id)
        message = await channel.fetch_message(int(message_id))
        await message.delete()


async def run() -> None:
    config = DiscordConfig()
    state = DiscordState(config.state_path)
    adapter = DiscordAdapter(config, state)
    reply_service = DiscordReplyService(adapter, state)
    app = create_reply_app(reply_service, config.adapter_secret)
    server = uvicorn.Server(
        uvicorn.Config(app, host=config.reply_host, port=config.reply_port, log_level="info")
    )
    try:
        await asyncio.gather(server.serve(), adapter.start(config.discord_bot_token))
    finally:
        await adapter.close()
        state.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(run())
