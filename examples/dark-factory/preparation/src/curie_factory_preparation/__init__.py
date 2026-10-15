"""Factory-owned local authoring contract; structural validity is not readiness."""

from .specification import CONTRACT_VERSION, parse_specification
from .ticket_set import parse_ticket_set

__all__ = ["CONTRACT_VERSION", "parse_specification", "parse_ticket_set"]
