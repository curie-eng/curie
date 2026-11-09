"""Provider-backed sender identities behind the frozen sender port."""

import asyncio
import logging
import time
from collections.abc import Callable, Mapping
from typing import Final

from slack_sdk.web.async_client import AsyncWebClient

from ..ports.reads import SenderIdentities

logger = logging.getLogger(__name__)
_AUTH_TEST_TIMEOUT_S: Final = 5.0
_AUTH_TEST_RETRY_S: Final = 300.0


class SlackSenderIdentities:
    """Each Slack identity's bot user id, from ``auth.test`` on its own token.

    Asked lazily, on the first lookup, so a worker whose Slack is unreachable
    still boots; an identity that did not answer is asked again no sooner than
    ``retry_after_s`` later, and until then its turns are not counted.

    ``identity_of`` never waits on ``auth.test``: a person's Slack turn must
    not pay for a sibling's lookup (ADR-0168 decision 6, finding 4). While an
    identity's bot user is not yet known, or a refresh is already in flight,
    the answer is None -- fail open, treated as not-sibling -- and a refresh
    runs in the background so a later turn is counted once it lands.
    """

    def __init__(
        self,
        tokens: Mapping[str, str],
        *,
        base_url: str | None = None,
        timeout_s: float = _AUTH_TEST_TIMEOUT_S,
        retry_after_s: float = _AUTH_TEST_RETRY_S,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._tokens = {name: token for name, token in tokens.items() if token.strip()}
        self._base_url = base_url or None
        self._timeout_s = timeout_s
        self._retry_after_s = retry_after_s
        self._clock = clock
        self._by_user: dict[str, str] = {}
        self._known: set[str] = set()
        self._next_try: dict[str, float] = {}
        self._refresh_task: asyncio.Task[None] | None = None

    @property
    def identities(self) -> tuple[str, ...]:
        return tuple(self._tokens)

    async def identity_of(self, author: str) -> str | None:
        if self._due() and (self._refresh_task is None or self._refresh_task.done()):
            self._refresh_task = asyncio.create_task(self._refresh())
        return self._by_user.get(author)

    async def _refresh(self) -> None:
        due = self._due()
        if not due:
            return
        answers = await asyncio.gather(*(self._ask(name) for name in due))
        for name, user_id in zip(due, answers, strict=True):
            if user_id is None:
                self._next_try[name] = self._clock() + self._retry_after_s
                logger.warning(
                    "Slack identity %s: auth.test did not answer; turns its bot "
                    "writes are not counted against the sibling limit yet",
                    name,
                )
                continue
            self._known.add(name)
            self._by_user[user_id] = name

    def _due(self) -> list[str]:
        now = self._clock()
        return [
            name
            for name in self._tokens
            if name not in self._known and self._next_try.get(name, 0.0) <= now
        ]

    async def _ask(self, name: str) -> str | None:
        kwargs: dict[str, object] = {
            "token": self._tokens[name],
            "timeout": max(1, int(self._timeout_s)),
        }
        if self._base_url is not None:
            kwargs["base_url"] = self._base_url
        client = AsyncWebClient(**kwargs)  # type: ignore[arg-type]
        try:
            response = await asyncio.wait_for(client.auth_test(), timeout=self._timeout_s)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - existing broad catch retained
            return None
        user_id = response.get("user_id") if response.get("ok") is True else None
        return user_id if isinstance(user_id, str) and user_id else None


_CONFORMS: type[SenderIdentities] = SlackSenderIdentities
