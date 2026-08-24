"""Environment configuration for the Discord adapter."""

import json
from pathlib import Path

from pydantic import Field, TypeAdapter
from pydantic_settings import BaseSettings, SettingsConfigDict

from .ingress import DiscordBinding


class DiscordConfig(BaseSettings):
    model_config = SettingsConfigDict(
        extra="ignore", populate_by_name=True, validate_default=True
    )

    discord_bot_token: str = Field(
        default="", min_length=1, validation_alias="DISCORD_BOT_TOKEN"
    )
    adapter_secret: str = Field(
        default="", min_length=1, validation_alias="CURIE_DISCORD_ADAPTER_SECRET"
    )
    curie_api_url: str = Field(
        default="http://localhost:8000", validation_alias="CURIE_API_URL"
    )
    bindings: list[DiscordBinding] = Field(
        default_factory=list, validation_alias="CURIE_DISCORD_BINDINGS"
    )
    bindings_path: Path | None = Field(
        default=None, validation_alias="CURIE_DISCORD_BINDINGS_PATH"
    )
    state_path: Path = Field(
        default=Path("/var/lib/curie-discord/state.sqlite3"),
        validation_alias="CURIE_DISCORD_STATE_PATH",
    )
    reply_host: str = Field(default="0.0.0.0", validation_alias="CURIE_DISCORD_REPLY_HOST")
    reply_port: int = Field(default=8080, validation_alias="CURIE_DISCORD_REPLY_PORT")
    placeholder_text: str = Field(
        default="On it. Working on your request.",
        validation_alias="CURIE_DISCORD_PLACEHOLDER_TEXT",
    )

    def load_bindings(self) -> list[DiscordBinding]:
        """Read the mounted binding map on every ingress selection.

        Environment JSON remains the small local default. Production can mount
        a file and rotate scoped tokens without restarting the Gateway client.
        """

        if self.bindings_path is None:
            return list(self.bindings)
        raw = json.loads(self.bindings_path.read_text())
        return TypeAdapter(list[DiscordBinding]).validate_python(raw)
