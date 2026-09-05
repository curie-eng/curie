"""Per-conversation credential authority for the runner's ACI control routes.

ADR-0116 decision 2 moves session identity onto the authenticated ``Event``
frame so a pre-booted runner can be bound to a conversation after its pod
exists; ADR-0122 bounds a warm pool's pre-baked token to *adoption and nothing
else*: the first authenticated ``Event`` that carries ``adoption_credential``
installs a fresh per-conversation credential and retires the bootstrap for that
pod. This module is the runner-side state machine for that rule.

Three credential modes, decided once at app construction:

- ``OPEN``: no token configured. The legacy pass-through (CLI, fake-model CI).
  Nothing is adoptable, nothing is gated.
- ``PER_CLAIM``: a token the worker minted per claim and injected as pod env.
  The runner was bound to its conversation at boot; the token is a conversation
  credential from the first request on and nothing is adoptable.
- ``BOOTSTRAP``: a pool bootstrap credential. Until adoption it authenticates
  exactly one thing, an adopting ``Event`` on ``/v1/event``; every other gated
  route refuses it. Adoption is atomic: exactly one concurrent request wins,
  the conversation binding is applied to the session first, and only then is
  the bootstrap replaced outright by the presented credential. A failed
  binding leaves the runner unbound and the bootstrap still adoptable, so a
  retry can succeed and no partial state is ever observable.

The presented credential material never appears in an exception message, a log
line, or a response body: refusals carry a fixed reason and an HTTP status.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum

logger = logging.getLogger("curie_runner.adoption")

BindConversation = Callable[[str, str | None], Awaitable[None]]


class CredentialMode(StrEnum):
    OPEN = "open"
    PER_CLAIM = "per-claim"
    BOOTSTRAP = "bootstrap"


class Principal(StrEnum):
    """What a presented bearer proves, at the moment it is checked."""

    NONE = "none"
    BOOTSTRAP = "bootstrap"
    CONVERSATION = "conversation"


@dataclass(frozen=True)
class ConversationBinding:
    """The conversation an adopted runner is bound to (identity, never secret)."""

    session_id: str
    history_ref: str | None


class AdoptionRefused(Exception):
    """An adoption request was refused; ``status``/``error`` are safe to return."""

    def __init__(self, status: int, error: str) -> None:
        super().__init__(error)
        self.status = status
        self.error = error


def _compare(presented: str | None, active: bytes | None) -> bool:
    if presented is None or active is None:
        return False
    # Compare UTF-8 bytes: hmac.compare_digest raises TypeError on a non-ASCII
    # str, which aiohttp would surface as a 500 instead of a 401.
    return hmac.compare_digest(presented.encode("utf-8"), active)


class CredentialAuthority:
    """The one active credential for this runner process, and how it changes.

    ``token`` is the per-claim conversation credential (``CURIE_RUNNER_TOKEN``);
    ``bootstrap_token`` is a pool bootstrap credential. A per-claim token wins
    when both are supplied: the pod was bound at boot, so the bootstrap must not
    be accepted at all. Neither configured is the legacy open mode.
    """

    def __init__(self, *, token: str | None = None, bootstrap_token: str | None = None) -> None:
        # A falsy value means "not configured": an empty token would make
        # ``Bearer `` with an empty value compare-equal.
        if token:
            self._mode = CredentialMode.PER_CLAIM
            self._active: bytes | None = token.encode("utf-8")
        elif bootstrap_token:
            self._mode = CredentialMode.BOOTSTRAP
            self._active = bootstrap_token.encode("utf-8")
        else:
            self._mode = CredentialMode.OPEN
            self._active = None
        self._binding: ConversationBinding | None = None
        self._lock = asyncio.Lock()

    @property
    def mode(self) -> CredentialMode:
        return self._mode

    @property
    def gated(self) -> bool:
        """Whether the control routes require a bearer at all."""

        return self._mode is not CredentialMode.OPEN

    @property
    def binding(self) -> ConversationBinding | None:
        """The adopted conversation, or None before adoption / outside bootstrap mode."""

        return self._binding

    @property
    def adoptable(self) -> bool:
        return self._mode is CredentialMode.BOOTSTRAP and self._binding is None

    def authenticate(self, presented: str | None) -> Principal:
        """Classify a presented bearer against the credential active right now.

        After adoption the bootstrap is simply no longer the active credential,
        so it classifies as ``NONE`` exactly like any other wrong token; nothing
        distinguishes a retired bootstrap from a guess.
        """

        if not self.gated or not _compare(presented, self._active):
            return Principal.NONE
        if self._mode is CredentialMode.BOOTSTRAP and self._binding is None:
            return Principal.BOOTSTRAP
        return Principal.CONVERSATION

    async def adopt(
        self,
        credential: str,
        session_id: str,
        history_ref: str | None,
        *,
        bind: BindConversation,
    ) -> ConversationBinding:
        """Bind this runner to one conversation and retire the bootstrap, atomically.

        ``bind`` applies the conversation to the live session (history load,
        session rebuild, attestation) and may raise; when it does, nothing here
        has changed and the bootstrap remains adoptable. The credential swap is
        the LAST step, after the binding is applied, so an observer can never
        see the new credential accepted before the conversation is bound, nor
        the bootstrap accepted after it is.

        The locked transition runs shielded from the caller's cancellation: a
        client that disconnects (or a request task cancelled) mid-bind must
        not be able to leave the session half-adopted with the bootstrap still
        active. The caller still observes ``CancelledError``; the transition
        itself runs to one of its two consistent ends, bound-and-swapped or
        rolled-back-and-adoptable.
        """

        return await asyncio.shield(
            self._adopt_locked(credential, session_id, history_ref, bind=bind)
        )

    async def _adopt_locked(
        self,
        credential: str,
        session_id: str,
        history_ref: str | None,
        *,
        bind: BindConversation,
    ) -> ConversationBinding:
        async with self._lock:
            if self._mode is not CredentialMode.BOOTSTRAP:
                raise AdoptionRefused(409, "runner is not adoptable")
            if self._binding is not None:
                raise AdoptionRefused(409, "runner is already bound to a conversation")
            if not session_id:
                raise AdoptionRefused(400, "adoption requires a session_id")
            if not credential or not credential.strip():
                # The wire layer already rejects an empty or whitespace
                # credential; this guard keeps the authority itself from ever
                # installing one, which would make an empty bearer authenticate.
                raise AdoptionRefused(400, "adoption credential must not be empty")
            if _compare(credential, self._active):
                # Installing the bootstrap as the conversation credential would
                # leave the shared secret in force for a bound conversation.
                raise AdoptionRefused(400, "adoption credential must differ from the bootstrap")
            try:
                await bind(session_id, history_ref)
            except Exception as exc:  # noqa: BLE001 - any binding failure is inert
                logger.error(
                    "adoption binding failed session=%s error_class=%s",
                    session_id,
                    type(exc).__name__,
                )
                raise AdoptionRefused(503, "conversation could not be bound") from exc
            binding = ConversationBinding(session_id=session_id, history_ref=history_ref)
            self._binding = binding
            self._active = credential.encode("utf-8")
            logger.info("adoption applied session=%s; bootstrap credential retired", session_id)
            return binding
