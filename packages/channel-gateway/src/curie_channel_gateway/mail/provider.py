"""The mail provider interface: the one seam between the adapter and a mail service.

ADR 0228. The neutral adapter (everything else in this package) keeps the
channel protocol, delivery state, the inbound gate, the intake path and
approvals by email. A mail provider talks to one mail service: it lists and
reads messages, sends replies in a thread, looks for the reply marker in a
thread, and reports what the receiving mail system knows about who sent a
message. AgentMail is the built-in provider (``agentmail.AgentMailProvider``).
Any other provider ships outside this repository as a Python package that
registers a factory in the ``curie.mail_provider`` entry-point group.

No operation raises for a service or network failure: each returns its result
or a :class:`Failure` carrying one of the neutral failure kinds below. The
interface is internal to the adapter. It is not a frozen contract and is not
generated to other languages; it changes through ordinary review, with
``provider_conformance`` as the gate.
"""

from __future__ import annotations

import importlib.metadata
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Literal, Protocol, runtime_checkable

# major.minor. An addition an older provider can safely lack raises the minor and
# names what the adapter does without it; any change to the meaning of something
# that exists raises the major.
INTERFACE_VERSION: tuple[int, int] = (1, 0)

ENTRY_POINT_GROUP = "curie.mail_provider"
BUILTIN_PROVIDER = "agentmail"
PROVIDER_ENV = "CURIE_MAIL_PROVIDER"

FailureKind = Literal[
    "unreachable",
    "egress_refused",
    "rate_limited",
    "refused",
    "server_error",
    "too_large",
]
FAILURE_KINDS: tuple[FailureKind, ...] = (
    "unreachable",
    "egress_refused",
    "rate_limited",
    "refused",
    "server_error",
    "too_large",
)
# The longest cause a failure may carry. A cause must also hold no credential and
# no mail content; the adapter bounds and redacts it again before logging.
FAILURE_CAUSE_MAX_CHARS = 120

VerdictOutcome = Literal["verified", "failed", "unverifiable"]
ThreadAnswer = Literal["yes", "no", "deleted"]


@dataclass(frozen=True)
class Failure:
    """A neutral failure in place of an HTTP status.

    Attributes:
        kind: what failed: ``unreachable`` (transport), ``egress_refused`` (the
            connection was refused), ``rate_limited``, ``refused`` (the service
            said no), ``server_error`` or ``too_large``.
        cause: a short, bounded description with no credential and no mail
            content, or empty.
        retry_after: for ``rate_limited``, the delay in seconds the service
            asked for, or None.
    """

    kind: FailureKind
    cause: str = ""
    retry_after: float | None = None


@dataclass(frozen=True)
class MessageSummary:
    """One listed message, before any body is fetched.

    Attributes:
        message_id: an opaque id, stable across reads and restarts.
        thread_id: an opaque id shared by a message and every reply in its thread.
        sent_by_inbox: whether this inbox sent the message.
        senders: the From mailboxes as listed. Claimed, never trusted.
    """

    message_id: str
    thread_id: str
    sent_by_inbox: bool
    senders: tuple[str, ...]


@dataclass(frozen=True)
class MessagePage:
    """One page of a listing, newest first, and the cursor for the next page."""

    messages: tuple[MessageSummary, ...]
    next_cursor: str | None


@dataclass(frozen=True)
class SenderVerdict:
    """Whether the receiving mail system authenticated the sender.

    Attributes:
        outcome: ``verified``, ``failed`` or ``unverifiable``.
        domain: for ``verified``, the RFC5322.From domain DMARC evaluated (the
            ``header.from`` of the receiving system's DMARC result).
        reason: for ``failed``, a short reason with no mail content.
    """

    outcome: VerdictOutcome
    domain: str = ""
    reason: str = ""


UNVERIFIABLE = SenderVerdict("unverifiable")


@dataclass(frozen=True)
class Message:
    """One read message, normalized.

    Attributes:
        message_id: the same id the listing gave.
        thread_id: the thread the message belongs to.
        subject: the subject, or empty.
        senders: every claimed From mailbox.
        to: the To addresses.
        cc: the Cc addresses.
        new_text: the new text with quoted history removed, or None when the
            service cannot separate it. No decision is read without it.
        text: the full plain text, or None.
        new_html: the new HTML, or None.
        html: the full HTML, or None.
        headers: the headers the service returned, or None when it returned
            none, which the adapter treats as sent automatically.
        verdict: the sender verdict.
    """

    message_id: str
    thread_id: str
    subject: str
    senders: tuple[str, ...]
    to: tuple[str, ...]
    cc: tuple[str, ...]
    new_text: str | None
    text: str | None
    new_html: str | None
    html: str | None
    headers: Mapping[str, str] | None
    verdict: SenderVerdict


@dataclass(frozen=True)
class Accepted:
    """The service took a reply for delivery."""


ACCEPTED = Accepted()


@runtime_checkable
class MailProvider(Protocol):
    """One mail service, as the neutral adapter needs it.

    A provider declares, once, when it is built: ``interface_version``,
    ``inbox_address`` (the one bare address this mailbox receives at),
    ``verifies_senders`` (false means every read returns ``unverifiable``) and
    ``sent_visible_within`` (zero for read-after-write visibility of a sent
    reply, otherwise a bound in seconds).
    """

    @property
    def name(self) -> str:
        """The provider's name, as ``CURIE_MAIL_PROVIDER`` selects it."""
        ...

    @property
    def interface_version(self) -> tuple[int, int]:
        """The interface version this provider implements, as major and minor."""
        ...

    @property
    def inbox_address(self) -> str:
        """The one bare address this mailbox receives at."""
        ...

    @property
    def verifies_senders(self) -> bool:
        """Whether a read can ever carry a ``verified`` verdict."""
        ...

    @property
    def sent_visible_within(self) -> float:
        """Zero for read-after-write visibility of a sent reply, else a bound in seconds."""
        ...

    def list_messages(self, cursor: str | None, limit: int) -> MessagePage | Failure:
        """List one page of mail, newest first, with the provider's safe filters."""
        ...

    def get_message(self, message_id: str) -> Message | Failure:
        """Read one message, with its sender verdict."""
        ...

    def send_reply(self, message_id: str, text: str, reply_all: bool) -> Accepted | Failure:
        """Reply in the message's thread, to the sender or with ``reply_all`` to everyone.

        Accepted means the service took the reply for delivery and, when
        ``sent_visible_within`` is zero, ``thread_contains`` finds its marker
        from the moment this returns.
        """
        ...

    def thread_contains(self, thread_id: str, marker: str) -> ThreadAnswer | Failure:
        """Whether a message in the thread carries ``marker``.

        ``deleted`` only on the service's own definitive answer. Anything
        ambiguous is a failure, which the adapter reads as unreadable.
        """
        ...

    def config_problems(self) -> list[str]:
        """Missing or invalid provider settings, each naming its variable."""
        ...


ProviderFactory = Callable[[], MailProvider]


class ProviderLoadError(RuntimeError):
    """The configured provider cannot be loaded; the message names the variable."""


def check_interface_version(provider: MailProvider) -> str | None:
    """Return why the provider's interface version cannot boot, or None.

    The adapter boots a provider with the same major and an equal or older
    minor, and refuses a different major or a newer minor, naming both.
    """
    version = getattr(provider, "interface_version", None)
    if (
        not isinstance(version, tuple)
        or len(version) != 2
        or not all(isinstance(part, int) and not isinstance(part, bool) for part in version)
    ):
        return f"{PROVIDER_ENV}: the provider declares no major.minor interface version"
    major, minor = version
    if major != INTERFACE_VERSION[0] or minor > INTERFACE_VERSION[1]:
        return (
            f"{PROVIDER_ENV}: the provider implements mail provider interface "
            f"{major}.{minor}, and this adapter supports "
            f"{INTERFACE_VERSION[0]}.0 to {INTERFACE_VERSION[0]}.{INTERFACE_VERSION[1]}"
        )
    return None


def resolve_factory(
    name: str,
    builtin: ProviderFactory,
    entry_points: Callable[[], importlib.metadata.EntryPoints] | None = None,
) -> ProviderFactory:
    """The factory for the provider called ``name``.

    ``agentmail`` resolves to the built-in by direct import, never through
    discovery, so a broken installed package cannot take the built-in down.
    Any other name is looked up in the ``curie.mail_provider`` entry-point
    group, and the lookup refuses, naming the variable, when no entry point
    claims the name, when more than one does, when one claims ``agentmail``, or
    when one points at a flat top-level module.

    Args:
        name: the configured provider name.
        builtin: the built-in AgentMail factory.
        entry_points: returns the group's entry points; defaults to the
            installed distributions.

    Returns:
        A factory that builds the provider from the environment.

    Raises:
        ProviderLoadError: the name cannot be resolved to exactly one factory.
    """
    wanted = name.strip()
    if not wanted:
        raise ProviderLoadError(f"{PROVIDER_ENV} is set but empty")
    if wanted == BUILTIN_PROVIDER:
        return builtin
    lister = entry_points or (lambda: importlib.metadata.entry_points(group=ENTRY_POINT_GROUP))
    found = list(lister())
    if any(entry.name == BUILTIN_PROVIDER for entry in found):
        raise ProviderLoadError(
            f"{PROVIDER_ENV}: an installed package claims the reserved provider name "
            f"{BUILTIN_PROVIDER!r} in the {ENTRY_POINT_GROUP!r} entry-point group"
        )
    matches = [entry for entry in found if entry.name == wanted]
    if not matches:
        raise ProviderLoadError(
            f"{PROVIDER_ENV}={wanted!r}: no installed package registers that provider "
            f"in the {ENTRY_POINT_GROUP!r} entry-point group"
        )
    if len(matches) > 1:
        raise ProviderLoadError(
            f"{PROVIDER_ENV}={wanted!r}: {len(matches)} installed packages register "
            f"that provider in the {ENTRY_POINT_GROUP!r} entry-point group"
        )
    entry = matches[0]
    module = entry.value.partition(":")[0]
    if "." not in module:
        raise ProviderLoadError(
            f"{PROVIDER_ENV}={wanted!r}: the entry point names the flat top-level module "
            f"{module!r}; register a factory inside a package"
        )
    try:
        factory = entry.load()
    except Exception as error:  # noqa: BLE001 - the load failure becomes a boot error
        raise ProviderLoadError(
            f"{PROVIDER_ENV}={wanted!r}: loading the provider failed: {type(error).__name__}"
        ) from error
    if not callable(factory):
        raise ProviderLoadError(
            f"{PROVIDER_ENV}={wanted!r}: the entry point does not name a callable factory"
        )
    return factory  # type: ignore[no-any-return]


def provider_problems(provider: MailProvider) -> list[str]:
    """The neutral checks every provider passes at boot, plus its own."""
    problems: list[str] = []
    version_problem = check_interface_version(provider)
    if version_problem:
        problems.append(version_problem)
    inbox = getattr(provider, "inbox_address", "")
    if not _is_bare_address(inbox):
        problems.append(
            f"{PROVIDER_ENV}: the provider's inbox address is empty or not one bare address"
        )
    bound = getattr(provider, "sent_visible_within", None)
    if not isinstance(bound, (int, float)) or isinstance(bound, bool) or bound < 0:
        problems.append(
            f"{PROVIDER_ENV}: the provider declares no non-negative sent-visibility bound"
        )
    if not isinstance(getattr(provider, "verifies_senders", None), bool):
        problems.append(f"{PROVIDER_ENV}: the provider does not declare verifies_senders")
    problems.extend(provider.config_problems())
    return problems


def _is_bare_address(value: object) -> bool:
    """Whether ``value`` is exactly one bare ``local@domain`` address."""
    if not isinstance(value, str) or value != value.strip() or not value:
        return False
    if any(character.isspace() or character in "<>,;\"'()" for character in value):
        return False
    local, at, domain = value.partition("@")
    return bool(at) and bool(local) and bool(domain) and "@" not in domain and "." in domain
