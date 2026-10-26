"""Dedicated series evidence observer; no v1 role widening, @spec COMMISSIONING-SERIES-3."""

from .worker_acl import _HANDSHAKE, _RESET

__all__ = ["evidence_observer_acl_rules"]

_RULES = (
    *_RESET,
    *_HANDSHAKE,
    "+acl|whoami",
    "+info|server",
    "+time",
    "+client|list",
    "(+get +type %R~protected:commissioning:permit:* %R~protected:commissioning:journal:* "
    "%R~protected:commissioning:reservation:* %R~protected:commissioning:ownership:* "
    "%R~protected:commissioning:admission:*)",
)


def evidence_observer_acl_rules() -> tuple[str, ...]:
    """Read records and connection presence on one owned broker only.

    Provisioning, credential custody and authenticated observations are separate
    obligations; a rule list itself grants nothing. @spec COMMISSIONING-SERIES-3.
    """
    return _RULES
