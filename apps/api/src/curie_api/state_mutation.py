"""One record per removal or rewrite of an agent's stored state (#3673).

The state and memory routers call :func:`record` after a mutation commits, so
an entry that disappears can be attributed to an operation, a scope and a kind
of credential. The line never carries the stored value or a memory entry's
content: those are the agent's data, and a log stream may be shipped somewhere
less protected than the database.

Every field is rendered into the message as well as attached as the
``state_mutation`` extra, because the service's JSON stderr stream keeps only
the rendered message.
"""

import logging
import uuid
from typing import Any, Literal

from curie_telemetry import record_metric

logger = logging.getLogger(__name__)

# The namespaces the platform owns. Every other namespace is caller-chosen, so
# the metric folds it into ``other`` rather than open a series per name.
_METRIC_NAMESPACES = frozenset({"memory", "transcript"})


def record(
    *,
    op: Literal["delete", "edit"],
    agent_id: uuid.UUID,
    scope: str | None,
    namespace: str,
    key: str,
    principal: str,
    removed: bool | None = None,
    index: int | None = None,
) -> None:
    """Log one mutation and count it when it changed stored state.

    ``removed`` is a delete's outcome (False when there was nothing to remove);
    an edit passes None. ``scope`` None is the agent's shared scope.
    """

    fields: dict[str, Any] = {
        "op": op,
        "agent_id": str(agent_id),
        "scope": scope if scope is not None else "shared",
        "namespace": namespace,
        "key": key,
    }
    if index is not None:
        fields["index"] = index
    if removed is not None:
        fields["removed"] = removed
    fields["principal"] = principal
    rendered = " ".join(
        f"{name}={str(value).lower() if isinstance(value, bool) else value}"
        for name, value in fields.items()
    )
    logger.info("state mutation %s", rendered, extra={"state_mutation": fields})
    if removed is False:
        return
    record_metric(
        "curie.state.mutation",
        attributes={
            "service.name": "curie-api",
            "op": op,
            "namespace": namespace if namespace in _METRIC_NAMESPACES else "other",
        },
    )
