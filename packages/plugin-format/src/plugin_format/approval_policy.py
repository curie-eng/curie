"""Normalize the manifest's grantable approval-policy gates (#558).

The operator opt-in ``grantableViaPolicy`` marks a gate whose policy-gate
approval MAY mint a one-shot grant for the tool the gate names (its ``gate``
field, MANIFEST-supplied, never model-supplied). ``grantable_routes`` is the
SINGLE normalization shared by the deploy-time validator
(``plugin_format.validate``) and the runtime loader
(``curie_runner.approval.resolve_approval_policy``). Sharing one helper makes
the two paths identical *by construction* -- the #453/#544 lesson that a
validator and a runtime loader normalizing separately can silently disagree and
ship a fail-open.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path

import yaml
from pydantic import ValidationError

from .connectors import CONNECTORS_FILE, validate_connectors
from .manifest import resolve_manifest
from .models import ApprovalGate, McpConfig, PluginManifest
from .yaml_loader import safe_load_unique

# The live name of the platform-mounted publication tool (#2776). The runner
# mounts it beside its approval tool rather than shipping it in a bundle, so it
# is neither a bundle server nor a connector tool. The validator accepts this
# EXACT name as an approval gate, and the runner and worker arm and recognize
# the tool by it, so every side agrees on one spelling.
PLATFORM_PUBLISH_TOOL_NAME = "mcp__curie__publish_changes"


def grantable_routes(
    gates: list[ApprovalGate],
) -> tuple[dict[str, str], set[str]]:
    """Resolve the grantable ``{route: tool}`` map and the ambiguous routes.

    For each gate with ``grantableViaPolicy`` truthy AND a non-empty stripped
    ``gate`` AND a non-empty stripped ``route``, accumulate ``route`` -> the set
    of tools claiming it (``tool = gate.gate.strip()``, ``route =
    gate.route.strip()``). ``.strip()`` and case-SENSITIVE comparison mirror
    ``load_approval_policy`` so a config that validates green at deploy resolves
    identically at runtime (#453).

    Returns ``(resolved, ambiguous)``:

    - ``resolved`` maps each route whose tool-set holds exactly ONE distinct tool
      to that tool. A route named twice by the SAME tool is a duplicate, not a
      conflict: still one distinct tool, still resolved.
    - ``ambiguous`` is the set of routes claimed by MORE than one distinct tool.
      Such a route is excluded from ``resolved`` (arms no grant) and reported so
      the deploy validator can reject it, rather than validating green while
      arming nothing (the #453 shape).

    Non-grantable gates and gates with a blank ``gate`` or ``route`` are ignored.
    """

    tools_by_route: dict[str, set[str]] = {}
    for gate in gates:
        if not gate.grantableViaPolicy:
            continue
        tool = gate.gate.strip()
        route = gate.route.strip()
        if not tool or not route:
            continue
        tools_by_route.setdefault(route, set()).add(tool)

    resolved: dict[str, str] = {}
    ambiguous: set[str] = set()
    for route, tools in tools_by_route.items():
        if len(tools) == 1:
            resolved[route] = next(iter(tools))
        else:
            ambiguous.add(route)
    return resolved, ambiguous


# --- Operator gate-name normalization (#703): shared by the deploy validator ----
# and the runtime loader, so an operator-supplied gate name and a manifest gate
# name resolve to the SAME effective runtime form by construction (the #453/#544
# validator/runtime-drift lesson).


def effective_tool_prefix(bundle_name: str, server: str) -> str:
    """The live tool-name prefix the SDK plugin-namespacing produces (#703).

    The SAME template ``validate.py`` builds ``expected_prefixes`` from and
    ``effective_operator_gates`` matches against -- ONE definition shared by the
    deploy-time validator and the runtime loader so the prefix format cannot
    drift between them (the #453/#544 lesson).
    """

    return f"mcp__plugin_{bundle_name}_{server}__"


def connector_tool_prefix(server: str) -> str:
    """The live tool-name prefix a ``connectors.yaml`` server produces (#1495).

    A connector is NOT a plugin-loaded MCP server. The runner mounts it directly
    on ``ClaudeAgentOptions.mcp_servers`` (``curie_runner.connectors``,
    ADR-0086), on the same channel as Curie's own platform servers ``curie`` and
    ``curie-state``, so the SDK names its tools ``mcp__<server>__<tool>`` with NO
    ``plugin_<bundle>_`` infix -- exactly like
    ``approval.APPROVAL_TOOL_NAME``. Giving connectors the plugin prefix would
    arm a literal the runtime never produces: a gate that validates green and
    silently never fires, the #453 fail-open shape.

    One definition, shared by the deploy validator and the runtime loader, for
    the same anti-drift reason as ``effective_tool_prefix``.
    """

    return f"mcp__{server}__"


def _longest_matching_server(
    name: str, servers: set[str], prefix_of: Callable[[str], str]
) -> str | None:
    """The declared server whose ``prefix_of(server)`` matches ``name``, or ``None``.

    A server matches when ``name`` starts with ``prefix_of(server)`` AND has a
    non-empty remainder after it (``len(name) > len(prefix)``). When more than
    one declared server matches (one server name is a prefix of another), the
    LONGEST server name wins the tie -- the more specific match.
    """

    best_server: str | None = None
    for s in servers:
        prefix = prefix_of(s)
        if name.startswith(prefix) and len(name) > len(prefix):
            if best_server is None or len(s) > len(best_server):
                best_server = s
    return best_server


def _mcp_server_names(obj: object) -> set[str] | None:
    """The set of server names an mcp declaration object names, or ``None``.

    Accepts both a full config object (``{"mcpServers": {...}}``) and a bare
    servers map (``{name: server}``), matching ``validate._validate_mcp_object``'s
    payload wrapping so the name derivation is identical on both sides. ``None``
    means the declaration failed to validate (unreadable), which poisons the
    declared-server union.
    """

    payload = obj if isinstance(obj, dict) and "mcpServers" in obj else {"mcpServers": obj}
    try:
        config = McpConfig.model_validate(payload)
    except ValidationError:
        return None
    return set(config.mcpServers)


def declared_mcp_server_names(root: str | Path) -> set[str] | None:
    """The MCP server names a bundle declares, or ``None`` when unknowable.

    Reads the manifest's inline ``mcpServers`` object AND the root ``.mcp.json``,
    returning the union of every server name across both. ``None`` is the poison
    value ``validate._validate_mcp`` uses: a declaration existed but could not be
    read (invalid JSON, a config that failed to validate, or the path-string form
    the real loader ignores), so the declared-server set is unknowable and a gate
    cross-check must fail closed rather than assert against a partial set. An empty
    set is the distinct fact that a declaration was read and named no servers.
    """

    root = Path(root)
    servers: set[str] = set()
    unreadable = False

    manifest_path = resolve_manifest(root)
    if manifest_path is not None:
        try:
            data = json.loads(manifest_path.read_text(encoding="utf-8"))
            manifest = PluginManifest.model_validate(data)
        except (json.JSONDecodeError, UnicodeDecodeError, OSError, ValidationError):
            # The manifest itself is unreadable, so its declared servers are
            # unknowable -- poison the whole set.
            return None
        declared = manifest.mcpServers
        if isinstance(declared, dict):
            result = _mcp_server_names(declared)
            if result is None:
                unreadable = True
            else:
                servers |= result
        elif isinstance(declared, str):
            # The path-string form parses but the real loader ignores it, so the
            # servers never register: unknowable, not empty.
            unreadable = True

    root_mcp = root / ".mcp.json"
    if root_mcp.is_file():
        try:
            data = json.loads(root_mcp.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            unreadable = True
        else:
            result = _mcp_server_names(data)
            if result is None:
                unreadable = True
            else:
                servers |= result

    if unreadable:
        return None
    return servers


def connector_server_names(root: str | Path) -> set[str] | None:
    """The MCP server names the bundle's ``connectors.yaml`` supplies, or ``None`` (#1495).

    The THIRD source of a bundle's tool surface, alongside the manifest's inline
    ``mcpServers`` and the root ``.mcp.json`` that ``declared_mcp_server_names``
    reads. It is a separate function, not another branch of that one, because a
    connector's live tool name is namespaced DIFFERENTLY
    (``connector_tool_prefix``, no ``plugin_<bundle>_`` infix): folding the two
    name sets together would build the wrong prefix for one of them.

    Parsing goes through ``connectors.validate_connectors`` -- the same parser the
    deploy validator and the runner's connector mount use -- so the names a gate
    may be namespaced to are exactly the names Curie will mount.

    ``None`` is the same poison value ``declared_mcp_server_names`` returns: a
    ``connectors.yaml`` exists but could not be read or did not validate, so the
    connector-server set is unknowable and a gate cross-check must fail closed
    rather than assert against a partial set. ``_validate_connectors`` already
    errors on that file, so the gate check stays silent rather than stacking a
    second, misleading error. An empty set is the distinct fact that the bundle
    declares no connectors.
    """

    path = Path(root) / CONNECTORS_FILE
    if not path.is_file():
        return set()
    try:
        data = safe_load_unique(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError):
        return None
    parsed, errors = validate_connectors(data)
    if parsed is None or errors:
        # A rejected connectors.yaml is never mounted, so its names are not a
        # tool surface a gate may be namespaced to.
        return None
    return set(parsed.connectors)


def effective_operator_gates(
    bundle_name: str | None,
    servers: set[str] | None,
    name: str,
    *,
    connector_servers: set[str] | None,
) -> frozenset[str] | None:
    """Map an operator gate name to the effective runtime tool name(s) it arms (#703).

    The SDK plugin-prefixes a bundle MCP tool to
    ``mcp__plugin_<bundle>_<server>__<tool>``. An operator who writes the natural
    shorthand ``mcp__<server>__<tool>`` in ``CURIE_APPROVAL_REQUIRED_TOOLS`` must
    have it rewritten to that effective form, or the gate arms a literal that the
    runtime name never matches (a silent fail-open).

    Every naming rule below CONTRIBUTES to one union; none of them returns on its own
    match. That shape is the whole point of this function, and an early return is the
    defect it keeps having: the rules all read the same ``mcp__<...>__<tool>`` shape,
    so one gate name routinely satisfies several of them, and NOTHING in the inputs
    says which server actually hosts the tool. Returning on the first rule that
    matches therefore picks a reading by branch ORDERING, and ``build_can_use_tool``
    matches by exact string equality, so every live name the ordering did not pick
    gates nothing at all, silently (#1495, #1564). Arming an extra name costs at most
    one approval card for a tool nobody calls; missing one is a total fail-open. Do
    not reintroduce an early return here.

    Returns the NON-EMPTY ``frozenset`` of live names to arm -- the union of:

    - ``{name}`` for a built-in with no ``mcp__`` prefix, armed by raw name and never
      server-checked. The one rule that short-circuits, because a name without the
      ``mcp__`` prefix can satisfy no other rule;
    - ``{name}`` when ``name`` is ``mcp__<connector>__<tool>`` naming a connector the
      bundle declares in ``connectors.yaml`` (#1495). A connector rides the SDK's
      ``mcp_servers`` map directly, so that shorthand IS already the live runtime
      name: it is verified against the declared connectors and armed unchanged, never
      rewritten to the plugin form the runtime never produces;
    - ``{rewritten}`` when ``name`` is ``mcp__<server>__<tool>`` and ``<server>`` is
      a declared bundle server. ``<server>`` is resolved by MATCHING the shorthand
      against the declared servers (a server name may itself contain ``__``, so
      splitting at the first ``__`` would misparse ``mcp__foo__bar__do`` as server
      ``foo``); the longest matching server name wins when one is a prefix of
      another. The effective prefix is built exactly as ``validate.py`` constructs
      ``expected_prefixes``;
    - ``{name}`` when ``name`` is already ``mcp__plugin_``-prefixed AND matches an
      expected prefix ``mcp__plugin_<bundle>_<server>__`` for a declared server (with
      a non-empty tool remainder), mirroring ``validate.py``'s ``expected_prefixes``
      check -- an already-prefixed name is NOT trusted blindly (a typo'd
      ``mcp__plugin_wrongbundle_wrongserver__tool`` would arm a literal the runtime
      never matches, a fail-open).

    The last two rules can BOTH read one name, which is why the prefixed rule joins
    the union instead of returning: ``mcp__plugin_b_github__update_issue`` is the live
    form for server ``github`` AND the shorthand for a server named
    ``plugin_b_github`` (live form ``mcp__plugin_b_plugin_b_github__update_issue``),
    so both are armed (#1564). Likewise the connector and shorthand rules, for a
    connector and an MCP server of DIFFERENT names.

    Returns ``None`` -- "cannot verify", which the caller turns into a loud
    fail-closed boot error (#520) -- when the union comes out empty, or when the
    inputs are refused outright:

    - ``servers`` or ``connector_servers`` is ``None`` (an unreadable MCP or
      connectors declaration), so no ``mcp__``-shaped name is verifiable;
    - ``name`` matches a declared connector AND a declared MCP server of the SAME
      name (#1564): an invalid CONFIGURATION, not an unknowable one, which must keep
      failing loudly rather than arming a union;
    - that same double match with a falsy ``bundle_name``: the plugin half cannot be
      constructed, and arming the connector half alone would re-create the shadow;
    - an ``mcp__``-shaped name no rule verifies: an undeclared server, no declared
      servers, an empty tool remainder, a falsy ``bundle_name`` on a name only the
      plugin rules could read, or an already-prefixed name matching no expected
      prefix. The operator override is never deploy-validated, so this runtime check
      is its sole defense -- "cannot verify" fails CLOSED, not through.

    An empty ``frozenset`` is never returned: it would read as "armed nothing",
    which is the fail-open shape this helper exists to prevent. Refusal is ``None``.
    """

    # A built-in tool (Bash, Write, ...) carries no mcp__ prefix: armed by raw
    # name, never rewritten and never server-checked. No rule in the union below can
    # read such a name, so there is nothing to union it with.
    if not name.startswith("mcp__"):
        return frozenset({name})
    # Every mcp__-shaped name is verified against the declared-server sets. Without
    # them (an unreadable MCP or connectors declaration) nothing can be verified, so
    # fail closed -- this runtime check is the operator override's only defense.
    if servers is None or connector_servers is None:
        return None
    # A connectors.yaml server is mounted straight onto the SDK's mcp_servers map
    # (ADR-0086), so mcp__<connector>__<tool> is ALREADY the live runtime name; a
    # plugin server's mcp__<server>__<tool> shorthand must be REWRITTEN to
    # mcp__plugin_<bundle>_<server>__<tool>. Both rules test the SAME
    # mcp__<server>__ prefix shape (connector_tool_prefix vs the shorthand template
    # below), so ONE name can match both sets -- with connector `deploy` and MCP
    # server `deploy__prod` both declared, mcp__deploy__prod__apply matches each.
    connector = _longest_matching_server(name, connector_servers, connector_tool_prefix)
    shorthand = _longest_matching_server(name, servers, lambda s: f"mcp__{s}__")
    if connector is not None and shorthand is not None:
        # Equal length means the connector and the MCP server carry the SAME name
        # (equal-length prefixes of one name under one template are the same
        # string), and their live forms differ. That is an invalid CONFIGURATION,
        # so refuse: the caller turns None into a loud boot error (#520).
        # validate._reject_connector_name_collisions already rejects this at
        # deploy, but the operator env knob is never deploy-validated, which is
        # what this defends.
        if len(connector) == len(shorthand):
            return None
        # Two DIFFERENT servers each match this one gate name. The plugin half of
        # the pair cannot be constructed without a bundle name, and arming the
        # connector half alone would re-create the very shadow the union closes, so
        # refuse outright rather than arm a partial union.
        if not bundle_name:
            return None

    # THE union. Every rule below ADDS the live name it would arm and none of them
    # returns, because a single gate name can legitimately be read by several of
    # them and nothing here says which server hosts the tool. Do NOT convert any of
    # these into an early return: build_can_use_tool matches by exact string
    # equality, so a live form the union omits gates nothing at all, silently, while
    # an extra one costs at most one approval card for a tool nobody calls (#1564).
    # A set dedupes when two readings land on the same literal, so the readings need
    # no special-casing when they agree.
    resolved: set[str] = set()
    # A connectors.yaml server is mounted straight onto the SDK's mcp_servers map
    # (ADR-0086), so its shorthand IS already the live name: armed unchanged, never
    # rewritten (#1495). This rule needs no bundle name, which is why a
    # connectors-only bundle without one still arms its gates.
    if connector is not None:
        resolved.add(name)
    if bundle_name:
        # An mcp__<server>__<tool> shorthand for a declared plugin server: <server>
        # was resolved above by matching against the declared servers rather than
        # splitting at the first __ (a server name may contain __), preferring the
        # longest match, with a non-empty tool remainder required.
        if shorthand is not None:
            tool = name[len(f"mcp__{shorthand}__") :]
            resolved.add(f"mcp__plugin_{bundle_name}_{shorthand}__{tool}")
        # Already the effective plugin-namespaced form: verify it matches an
        # expected prefix mcp__plugin_<bundle>_<server>__ for a declared server
        # (non-empty tool remainder), exactly as validate.py asserts. NOT trusted
        # verbatim -- a typo'd mcp__plugin_wrongbundle_wrongserver__tool would arm a
        # literal the runtime never emits. A connector may legally be named `plugin`
        # (RFC 1123 forbids `_` in a connector name, so the reverse cannot happen),
        # and its mcp__plugin__<tool> lands here too: it adds nothing when no
        # expected prefix matches, and the connector rule above already armed it.
        if name.startswith("mcp__plugin_"):
            bundle = bundle_name
            matched = _longest_matching_server(
                name, servers, lambda s: effective_tool_prefix(bundle, s)
            )
            if matched is not None:
                resolved.add(name)
    # An empty union means no rule verified this name: fail CLOSED with None rather
    # than return an empty frozenset, which would read as "armed nothing".
    if not resolved:
        return None
    return frozenset(resolved)
