"""Snapshot sealing key custody: the reserved names and the refusal reason.

@spec ACTION-EXECUTOR-16. ADR 0124 decision 1 requires the sealing key to reach
only the hosted connector. The first release recognizes it by two reserved
names: ``SNAPSHOT_SEALING_KEY`` and ``SNAPSHOT_SEALING_KEYS_RETAINED`` (retired
keys kept while their records are undoable, decision 3). The only accepted
declaration is a ``SecretRef`` (``{name, from_secret}``) on a connector: every
other form either hands the value to the deploy path or puts it where a runner
sandbox can read it.

One definition for every Python seam that enforces custody:

* API bundle intake (``curie_api.bundles``) and the agent ``secrets`` map
  (``curie_api.schemas.agents``) refuse the names with ``custody_reason``;
* the API's ``undoable`` derivation (``curie_api.action_undoable``) reads
  ``SEALING_KEY_NAME``;
* the worker's ``inject_connector_secrets`` withholds ``SEALING_KEY_NAMES``;
* API bundle intake finds references in the fields the MCP client expands
  from the sandbox environment with ``sealing_key_references``.

The Helm guard in ``charts/curie/templates/agent-connector-secrets.yaml`` lists
the same two names. ``SEALING_KEY_CUSTODY_REASON`` is the single wording of
the refusal so the CLI bundle check can mirror it verbatim.
"""

from __future__ import annotations

import re

SEALING_KEY_NAME = "SNAPSHOT_SEALING_KEY"
SEALING_KEYS_RETAINED_NAME = "SNAPSHOT_SEALING_KEYS_RETAINED"

SEALING_KEY_NAMES: frozenset[str] = frozenset({SEALING_KEY_NAME, SEALING_KEYS_RETAINED_NAME})

# The refusal, formatted with the offending name. Kept as one constant so every
# surface (API bundle intake, API agent secrets, and later the CLI) says the
# same thing.
SEALING_KEY_CUSTODY_REASON = (
    "{name} is a reserved snapshot sealing key: it must reach only the hosted "
    "connector, so it may be declared only as a SecretRef "
    "(`- name: {name}` with `from_secret:`) on that connector"
)


def is_sealing_key_name(name: str) -> bool:
    """Whether ``name`` is one of the reserved sealing key names."""

    return name in SEALING_KEY_NAMES


def custody_reason(name: str) -> str:
    """The refusal message for ``name`` declared in a form other than a SecretRef."""

    return SEALING_KEY_CUSTODY_REASON.format(name=name)


# A reference the sandbox could expand: `$` then an optional `{` and optional
# whitespace, then the name, ending at a word boundary. Covers `$NAME`,
# `${NAME}`, `${ NAME }`, `${NAME:-x}`, `${NAME-x}` and a reference nested in
# another's default (`${OTHER:-${NAME}}`), since each name is searched on its
# own rather than parsed out of an outer match. The trailing boundary keeps a
# longer name that merely starts with a reserved one (`SNAPSHOT_SEALING_KEYRING`)
# a different variable, as the environment treats it.
_REFERENCE_RES = {
    name: re.compile(r"\$\{?\s*" + re.escape(name) + r"(?![A-Za-z0-9_])")
    for name in sorted(SEALING_KEY_NAMES)
}


def sealing_key_references(text: str) -> list[str]:
    """The reserved names ``text`` references for expansion, in sorted order."""

    return [name for name, pattern in _REFERENCE_RES.items() if pattern.search(text)]
