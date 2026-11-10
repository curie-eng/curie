"""The one refusal type every channel read step raises."""

from __future__ import annotations


class ChannelReadRefused(Exception):
    """A named ``channel_read.*`` refusal; the router turns it into the response."""

    def __init__(
        self, status: int, code: str, message: str, *, retry_after: int | None = None
    ) -> None:
        super().__init__(code)
        self.status = status
        self.code = f"channel_read.{code}"
        self.message = message
        self.retry_after = retry_after
