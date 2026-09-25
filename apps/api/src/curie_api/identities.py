"""Which identities an installation declares (ADR-0168 decisions 1 and 3).

Decision 1 (#3102) makes this the chart's list. Until then the one Slack app is
the only Slack identity, and an out-of-process adapter's name cannot be listed
by the API, so other kinds keep the slug rule the write schema already applies.
"""

from aci_protocol.turn import DEFAULT_IDENTITY, SLACK_KIND


def declared_identities(kind: str) -> frozenset[str] | None:
    """The identities a binding of ``kind`` may name, or None when not enumerable."""

    if kind == SLACK_KIND:
        return frozenset({DEFAULT_IDENTITY})
    return None


def refuse_undeclared(kind: str, identity: str | None) -> None:
    """Raise if ``identity`` is not one this installation declares for ``kind``.

    Shared by `ChannelBindingWrite` and `PublicationCreate`, which both need
    the same check. Call with the RESOLVED
    identity (``route_identity(kind, adapter)``), never the raw column: an
    omitted Slack adapter means the default app, and comparing the raw
    ``None`` here would refuse the common case of not naming one at all. A
    no-op for a kind `declared_identities` cannot yet enumerate.
    """

    declared = declared_identities(kind)
    if declared is None or identity in declared:
        return
    names = ", ".join(repr(name) for name in sorted(declared))
    raise ValueError(
        f"{kind} identity {identity!r} is not declared by this installation, "
        f"which declares {names}."
    )
