"""Database access for console."""

import hashlib
import secrets
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from ..models import ConsoleSession, OidcLoginAttempt, Principal, Tenant
from ..oidc import OidcClaims, code_challenge, new_code_verifier, new_nonce, new_state

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


# --- generic OIDC login (#2908, ADR 0155 step 3) ----------------------------
#
# The login transaction and the principal it resolves to. The callback runs
# consume -> exchange -> validate -> resolve -> mint in that order, and each
# step here is written so a crash or a concurrent duplicate between two of them
# cannot yield a second use of one attempt or a second row for one identity.

#: How long a started login may take to come back from the IdP. Long enough for
#: a person to type a password and pass MFA, short enough that an abandoned
#: attempt stops being redeemable soon after.
OIDC_LOGIN_TTL = timedelta(minutes=10)

#: The single-tenant appliance's tenant, provisioned by migration 0051 at this
#: fixed id. Every OIDC principal lands here until issuer-to-tenant mapping
#: exists.
DEFAULT_TENANT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


@dataclass(frozen=True)
class OidcLoginStart:
    """What one new login attempt hands the browser leg, in plaintext, once."""

    state: str
    nonce: str
    code_challenge: str


@dataclass(frozen=True)
class OidcLoginSecrets:
    """What a consumed attempt gives back to the callback."""

    nonce: str
    code_verifier: str


async def create_oidc_login_attempt(session: AsyncSession) -> OidcLoginStart:
    """Persist a new login attempt and return the values for the IdP redirect.

    Only the hash of ``state`` is stored; the plaintext goes to the browser (in
    the redirect and the state cookie) and nowhere else. The verifier stays
    server-side: the browser sees only its S256 challenge, so an authorization
    code intercepted on the way back cannot be redeemed without this row.

    The route that calls this is unauthenticated, so expired attempts are
    pruned here: the table then holds at most the attempts of the last
    :data:`OIDC_LOGIN_TTL`, not every login ever started.
    """
    state = new_state()
    nonce = new_nonce()
    verifier = new_code_verifier()
    await session.execute(delete(OidcLoginAttempt).where(OidcLoginAttempt.expires_at <= func.now()))
    session.add(
        OidcLoginAttempt(
            state_hash=hash_console_credential(state),
            nonce=nonce,
            code_verifier=verifier,
            expires_at=datetime.now(UTC) + OIDC_LOGIN_TTL,
        )
    )
    await session.commit()
    return OidcLoginStart(state=state, nonce=nonce, code_challenge=code_challenge(verifier))


async def consume_oidc_login_attempt(
    session: AsyncSession, state: str
) -> OidcLoginSecrets | None:
    """Spend the attempt ``state`` names, or ``None`` if it cannot be spent.

    One conditional UPDATE, committed before the caller goes anywhere near the
    IdP: two callbacks racing on one state cannot both win, and a callback that
    later fails (bad token, IdP down) has still used the attempt up, so the
    same callback URL cannot be retried into a session. Unknown, consumed and
    expired all return ``None`` -- one indistinguishable failure.
    """
    result = await session.execute(
        update(OidcLoginAttempt)
        .where(
            OidcLoginAttempt.state_hash == hash_console_credential(state),
            OidcLoginAttempt.consumed_at.is_(None),
            OidcLoginAttempt.expires_at > func.now(),
        )
        .values(consumed_at=func.now())
        .returning(OidcLoginAttempt.nonce, OidcLoginAttempt.code_verifier)
    )
    row = result.one_or_none()
    await session.commit()
    if row is None:
        return None
    return OidcLoginSecrets(nonce=row.nonce, code_verifier=row.code_verifier)


async def resolve_principal(
    session: AsyncSession, claims: OidcClaims, *, tenant_id: uuid.UUID = DEFAULT_TENANT_ID
) -> Principal:
    """The principal ``claims`` identify, created on first sight. NOT committed.

    Matched on ``(tenant_id, issuer, subject)`` only -- never on email, which an
    IdP lets users change and which two IdPs can both assert. One
    ``INSERT .. ON CONFLICT DO UPDATE`` both creates and refreshes, so two
    first logins racing for one identity converge on one row instead of one of
    them failing on the unique key. The update touches the IdP-owned
    attributes and ``last_seen_at`` only: ``status`` is Curie's, so a disabled
    principal logging in again stays disabled.

    Left uncommitted so the caller can roll the refresh back when it refuses
    the login (an inactive principal or tenant): a refused login changes
    nothing.
    """
    moment = datetime.now(UTC)
    statement = (
        insert(Principal)
        .values(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            idp_issuer=claims.issuer,
            idp_subject=claims.subject,
            type="human",
            status="active",
            email=claims.email,
            display_name=claims.display_name,
            last_seen_at=moment,
        )
        .on_conflict_do_update(
            constraint="principals_tenant_issuer_subject_key",
            set_={
                "email": claims.email,
                "display_name": claims.display_name,
                "last_seen_at": moment,
            },
        )
        .returning(Principal)
    )
    result = await session.scalars(statement, execution_options={"populate_existing": True})
    return result.one()


async def principal_is_active(session: AsyncSession, principal: Principal) -> bool:
    """Whether ``principal`` and its tenant may currently hold a session.

    Checked at login AND on every principal-authenticated request, so disabling
    a principal or suspending a tenant takes effect on existing sessions
    immediately, not when they expire.
    """
    if principal.status != "active":
        return False
    tenant_status = await session.scalar(
        select(Tenant.status).where(Tenant.id == principal.tenant_id)
    )
    return tenant_status == "active"


async def create_principal_console_session(
    session: AsyncSession, principal: Principal, *, now: datetime | None = None
) -> tuple[str, ConsoleSession]:
    """Mint a console session for ``principal`` and commit.

    The same row shape, hashing and lifetime as an ADR-0083 session, so
    revocation and expiry work exactly as they do there. ``subject`` stays NULL:
    it is the approval identity, and an OIDC session has none (ADR-0106;
    principal-based approval is ADR 0155 step 10). The login-code half is
    required by the schema, so it is filled with the hash of a random value
    nobody is ever shown and stamped consumed, which means
    :func:`exchange_console_login_code` can never redeem it.

    Returns:
        ``(plaintext session token, row)``; only the token's hash is stored.
    """
    moment = now or datetime.now(UTC).replace(tzinfo=None)
    token = new_session_token()
    row = ConsoleSession(
        subject=None,
        principal_id=principal.id,
        login_code_hash=hash_console_credential(new_session_token()),
        login_code_expires_at=moment,
        session_token_hash=hash_console_credential(token),
        session_expires_at=moment + CONSOLE_SESSION_TTL,
        consumed_at=moment,
    )
    session.add(row)
    await session.commit()
    await session.refresh(row)
    return token, row


async def live_principal_session(session: AsyncSession, token: str) -> Principal | None:
    """The active principal a live OIDC session token authenticates, else ``None``.

    Layered on :func:`live_console_session`, so revocation and expiry are the
    ADR-0083 ones. A login-code session (no ``principal_id``) is not a
    principal session, and neither is one whose principal or tenant is no
    longer active.
    """
    row = await live_console_session(session, token)
    if row is None or row.principal_id is None:
        return None
    principal = await session.get(Principal, row.principal_id, populate_existing=True)
    if principal is None or not await principal_is_active(session, principal):
        return None
    return principal
