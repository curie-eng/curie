"""Database access for console."""

import hashlib
import secrets
from datetime import UTC, datetime, timedelta

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import ConsoleSession

# --- console sessions (ADR-0083, #1044) -------------------------------------
#
# The credential never enters the database: every read and write below goes
# through `hash_console_credential`, so a dump of `console_sessions` is useless to
# an attacker. Callers hold the plaintext only long enough to hand it to the
# operator (the code) or the browser (the token).

#: How long a minted login code stays redeemable. Short by design: it exists only
#: to be copied from a terminal into a browser once.
LOGIN_CODE_TTL = timedelta(minutes=10)


#: How long an established console session stays valid before a fresh login.
CONSOLE_SESSION_TTL = timedelta(hours=12)


def hash_console_credential(value: str) -> str:
    """The stored form of a login code or session token.

    SHA-256 rather than a password hash on purpose: these are high-entropy
    machine-generated values, not user-chosen secrets, so there is nothing to slow
    down a guessing attack against -- and a lookup happens on every session-authed
    request, where a deliberately slow KDF would be a denial-of-service surface.

    Args:
        value: The plaintext code or token.

    Returns:
        Lowercase hex SHA-256 of ``value``.
    """
    return hashlib.sha256(value.encode()).hexdigest()


def new_login_code() -> str:
    """A single-use login code an operator can copy out of a terminal."""
    return secrets.token_urlsafe(12)


def new_session_token() -> str:
    """A console session token. Longer than the code: it is never typed."""
    return secrets.token_urlsafe(32)


async def create_console_login_code(
    session: AsyncSession, *, subject: str, now: datetime | None = None
) -> tuple[str, ConsoleSession]:
    """Mint a login code and its pending session row.

    Args:
        session: The database session.
        subject: The administrator-selected identity this session will carry.
        now: Injectable clock, so expiry is testable without sleeping.

    Returns:
        ``(plaintext code, row)``. The plaintext is returned ONCE and never
        stored; only its hash is persisted.
    """
    if not subject.strip():
        raise ValueError("console session subject must not be blank")
    moment = now or datetime.now(UTC).replace(tzinfo=None)
    code = new_login_code()
    row = ConsoleSession(
        subject=subject,
        login_code_hash=hash_console_credential(code),
        login_code_expires_at=moment + LOGIN_CODE_TTL,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return code, row


async def exchange_console_login_code(
    session: AsyncSession, code: str, *, now: datetime | None = None
) -> tuple[str, ConsoleSession] | None:
    """Consume a login code and mint the session token it establishes.

    Single-use and expiry are enforced HERE rather than by the caller, so no
    endpoint can accidentally skip either. A code that is unknown, already
    consumed, expired, or whose row was revoked yields ``None`` -- one
    indistinguishable failure, so a caller cannot probe which codes exist.

    Args:
        session: The database session.
        code: The plaintext login code presented by the browser.
        now: Injectable clock.

    Returns:
        ``(plaintext session token, row)`` on success, else ``None``.
    """
    moment = now or datetime.now(UTC).replace(tzinfo=None)
    result = await session.execute(
        select(ConsoleSession).where(
            ConsoleSession.login_code_hash == hash_console_credential(code)
        )
    )
    row = result.scalar_one_or_none()
    if row is None:
        return None
    if row.consumed_at is not None or row.revoked_at is not None:
        return None
    if row.login_code_expires_at <= moment:
        return None

    token = new_session_token()
    row.session_token_hash = hash_console_credential(token)
    row.session_expires_at = moment + CONSOLE_SESSION_TTL
    row.consumed_at = moment
    await session.commit()
    await session.refresh(row)
    return token, row


async def live_console_session(
    session: AsyncSession, token: str, *, now: datetime | None = None
) -> ConsoleSession | None:
    """The session a token authenticates, or ``None`` if it does not authenticate one.

    "Live" means exchanged, unrevoked and unexpired. ADR-0106 consumes this
    store directly for console approval principals without widening platform
    API-key authentication; revocation and expiry therefore take effect on the
    next resolve attempt.

    Args:
        session: The database session.
        token: The plaintext session token from the cookie.
        now: Injectable clock.

    Returns:
        The live row, else ``None`` -- again one indistinguishable failure.
    """
    moment = now or datetime.now(UTC).replace(tzinfo=None)
    result = await session.execute(
        select(ConsoleSession).where(
            ConsoleSession.session_token_hash == hash_console_credential(token)
        )
    )
    row = result.scalar_one_or_none()
    if row is None or row.revoked_at is not None:
        return None
    if row.session_expires_at is None or row.session_expires_at <= moment:
        return None
    return row


async def revoke_console_session(
    session: AsyncSession, row: ConsoleSession, *, now: datetime | None = None
) -> ConsoleSession:
    """Revoke a session by stamping ``revoked_at``.

    A column write, which is the whole point of a stored session: the operator can
    kill one without rotating the platform key and restarting the API.
    """
    row.revoked_at = now or datetime.now(UTC).replace(tzinfo=None)
    await session.commit()
    await session.refresh(row)
    return row
