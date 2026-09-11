"""Offline, credential-free MCP load check (issue #337).

Run as ``python -m curie_runner.check`` with ``CURIE_PLUGIN_DIR`` pointing at a
Claude Code plugin bundle. The check:

1. validates the bundle via the frozen ``load_plugins`` (``PluginBundleError`` ->
   ``invalid_bundle``),
2. parses the bundle's **declared** MCP servers (``extract_declared``),
3. builds a real ``ClaudeSDKClient`` via ``build_options`` and ``connect()`` -- no
   query, no model turn -- then polls ``get_mcp_status()`` until the bundle's own
   servers settle, and
4. compares declared intent against the plugin-owned registered servers
   (``evaluate``), emitting exactly one JSON object to **stdout** (all logging to
   **stderr**) and exiting 0 (green) / 1 (red) / 2 (invalid_bundle).

There is **no credential path**: the spike verified ``connect()`` succeeds with
zero credential and an empty HOME, and this module never issues a ``query()``.
The runner<->CLI JSON seam is frozen in the plan (Section 3).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

import yaml
from aci_protocol import BootEnv
from claude_agent_sdk import ClaudeSDKClient
from plugin_format import resolve_manifest
from plugin_format.connectors import CONNECTORS_FILE, ConnectorSpec, validate_connectors
from plugin_format.yaml_loader import safe_load_unique

from .adapter import build_options
from .connectors import derive_mcp_servers
from .plugin import PluginBundleError, load_plugins

logger = logging.getLogger(__name__)

# A declared boot key. The CLI produces it from the same declaration (the
# generated `env_keys::CURIE_PLUGIN_DIR`) when it runs this check, so the
# consumer side reads it from the declaration too rather than retyping it (#488).
PLUGIN_DIR_ENV = BootEnv.env_key("plugin_dir")

CHECK_NAME = "mcp-load"
CHECK_VERSION = 1
_DEFAULT_TIMEOUT_S = 30
_POLL_INTERVAL_S = 1.0

_STRING_POINTER_HINT = (
    "plugin.json 'mcpServers' is a string pointer; the real loader silently "
    "ignores this form — inline the object"
)

_EXIT_CODES = {"green": 0, "red": 1, "invalid_bundle": 2}

# ``${VAR}`` placeholder in a remote server's header value (e.g. a Bearer token).
_HEADER_VAR_RE = re.compile(r"\$\{([^}]+)\}")


def _remote_advisory(name: str, cred_vars: list[str]) -> str:
    # A remote (``url``) server is unreachable by construction here: the check
    # container runs with ``--network none`` so a bundle cannot pass by phoning
    # home. That makes a red for a remote server ambiguous -- it may mean the
    # declaration is broken, or simply that the offline contract did its job --
    # and previously nothing said which (#1093).
    suffix = ""
    if cred_vars:
        suffix = " " + " ".join(f"--secret {var}" for var in cred_vars)
    return (
        f"remote server {name} cannot be reached offline; this check runs with "
        "no network on purpose, so a red here is expected and is NOT evidence "
        "the bundle is broken -- run `curie skill up"
        f"{suffix}` for the real end-to-end test"
    )


def _authed_advisory(name: str, cred_vars: list[str]) -> str:
    # ``--secret`` forwards an environment-variable NAME, not the server name, so
    # name the real credential var(s) when we could extract them; otherwise fall
    # back to a generic placeholder rather than emit a wrong concrete name.
    if cred_vars:
        secret_flags = " ".join(f"--secret {var}" for var in cred_vars)
    else:
        secret_flags = "--secret <NAME>"
    return (
        f"authed server {name} needs a credential and was not exercised offline; "
        "a green here proves wiring not the credential, and a red may be just a "
        f"missing token -- run `curie skill up {secret_flags}` for the real "
        "end-to-end test"
    )


def _connector_advisory(name: str, form: str, cred_vars: list[str]) -> str:
    # A `connectors.yaml` connector is declared intent, not an address: ADR-0086
    # has the bundle say WHAT it needs and the platform derive where it runs.
    # This check runs on the `skill` tier, which hosts nothing, so a purely
    # hosted connector is correctly absent here (#1093) and a red for it is a
    # statement about the tier rather than about the bundle. Say so, and name
    # the command that does host it, or the author debugs a bundle that is fine.
    suffix = ""
    if cred_vars:
        suffix = " " + " ".join(f"--secret {var}" for var in cred_vars)
    if form == "hosted":
        return (
            f"connector {name} is declared in {CONNECTORS_FILE} and Curie hosts it; "
            "this check hosts no connector, so it was not exercisable here and the "
            "red is about the tier, not the bundle -- run `curie cluster deploy` to "
            f"host it, or add `unhosted_url:` to point at a copy you already run{suffix}"
        )
    # `unhosted_url` IS an address this tier knows how to MOUNT (#1160), so the
    # connector really was tried -- but it is not an address this tier can
    # REACH: the check container runs with `--network none` by construction, so
    # `http://localhost:8765/mcp` is exactly as unreachable here as any remote
    # `url:`. Mirror `_remote_advisory`'s honesty rather than calling the result
    # "real": telling the author the red is about their connector, when it is
    # really about the check's network policy, recreates the very misdiagnosis
    # #2348 exists to prevent.
    return (
        f"connector {name} was mounted from its `unhosted_url`, but this check "
        "runs with no network on purpose, so a red here is expected and is NOT "
        "evidence the bundle is broken -- run `curie skill up"
        f"{suffix}` for the real end-to-end test"
    )


# --------------------------------------------------------------------------- #
# Declared-server extraction (bundle intent)
# --------------------------------------------------------------------------- #
def extract_declared(plugin_dir: str) -> list[dict[str, Any]]:
    """Parse declared MCP servers into ``{name, source, form, authed, cred_vars, remote}`` rows.

    Covers all three declaration forms (plan Section 3): a ``plugin.json``
    ``mcpServers`` object (``inline``), a ``mcpServers`` string pointer resolved
    relative to the bundle root (``string_pointer``; a missing pointed file still
    surfaces the intent), and a bare-root ``.mcp.json`` (``bare_file``, deduped by
    name against the above).

    ``authed`` marks a server that carries a credential -- a non-empty ``env`` map
    or (a remote server) a non-empty ``headers`` map -- so the report can flag that
    the credential-free offline check never exercised it. ``cred_vars`` names the
    actual credential environment-variable(s) the server needs (``env`` keys, or
    ``${VAR}`` placeholders in ``headers`` values) so the advisory can point at the
    real ``--secret <VAR>`` to forward. A missing/unparseable pointed file defaults
    ``authed`` to False and ``cred_vars`` to empty (the declaration is unavailable).
    """

    root = Path(plugin_dir)
    declared: list[dict[str, Any]] = []
    seen: set[str] = set()

    def _add(
        name: str,
        source: str,
        form: str,
        authed: bool,
        cred_vars: list[str],
        remote: bool = False,
    ) -> None:
        if name not in seen:
            declared.append(
                {
                    "name": name,
                    "source": source,
                    "form": form,
                    "authed": authed,
                    "remote": remote,
                    "cred_vars": cred_vars,
                }
            )
            seen.add(name)

    def _add_server(name: str, source: str, form: str, server: Any) -> None:
        _add(
            name,
            source,
            form,
            _is_authed(server),
            _cred_vars(server),
            _is_remote(server),
        )

    manifest_path = resolve_manifest(root)
    manifest = _read_json(manifest_path) if manifest_path is not None else None
    mcp = manifest.get("mcpServers") if isinstance(manifest, dict) else None

    if isinstance(mcp, dict):
        for name, server in mcp.items():
            _add_server(
                str(name),
                "plugin.json",
                "inline",
                server,
            )
    elif isinstance(mcp, str):
        pointed = _servers_map(_read_json(root / mcp))
        if pointed:
            for name, server in pointed.items():
                _add_server(
                    str(name),
                    "plugin.json",
                    "string_pointer",
                    server,
                )
        else:
            # Pointed file missing/unparseable: the intent (a string-pointer
            # declaration) still surfaces so the caller can drive a red verdict;
            # the declaration dict is unavailable, so authed/cred_vars default empty.
            manifest_name = manifest.get("name") if isinstance(manifest, dict) else None
            fallback = str(manifest_name) if manifest_name else mcp
            _add(fallback, "plugin.json", "string_pointer", False, [])

    bare = _servers_map(_read_json(root / ".mcp.json"))
    for name, server in bare.items():
        _add_server(
            str(name),
            ".mcp.json",
            "bare_file",
            server,
        )

    # `connectors.yaml` is the fourth declaration channel and the only one the
    # census used to drop, so a bundle whose sole server was a declared
    # connector reported `declared: 0` / green -- byte-identical to a bundle
    # declaring nothing, while being the documented diagnostic for missing tools
    # (#2348). Unioned LAST on purpose: `seen` already holds the MCP-config rows,
    # so a name in both channels is counted once. That collision is refused
    # earlier by `validate_bundle` (#1118); this dedupe is belt-and-braces and
    # deliberately prefers the MCP row if that refusal is ever relaxed.
    for name, spec in sorted(_read_connectors(root).items()):
        # One source of truth: `remote` is derived FROM the form rather than
        # recomputed from the spec, so the two cannot desync.
        form = _connector_form(spec)
        _add(
            name,
            CONNECTORS_FILE,
            form,
            _connector_authed(spec),
            _connector_cred_vars(spec),
            remote=form == "remote",
        )

    return declared


def _servers_map(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        servers = payload.get("mcpServers")
        if isinstance(servers, dict):
            return servers
    return {}


def _is_remote(server: Any) -> bool:
    """True for a server the check can never reach: it is declared by ``url``.

    The check container runs with ``--network none`` (a bundle must not be able
    to pass by phoning home), so a remote server cannot connect here no matter
    how it is configured. Recording it lets the report explain the red instead
    of leaving it indistinguishable from a genuine bundle defect.
    """

    return isinstance(server, dict) and bool(server.get("url"))


def _is_authed(server: Any) -> bool:
    """A server is authed if its declaration carries a credential.

    The signal is a NON-EMPTY ``env`` map (the stdio form, e.g. a ``${TOKEN}``
    value) or a NON-EMPTY ``headers`` map (the remote form, e.g. an
    ``Authorization`` header). An empty ``env: {}`` carries no credential.
    """

    if not isinstance(server, dict):
        return False
    env = server.get("env")
    if isinstance(env, dict) and env:
        return True
    headers = server.get("headers")
    return isinstance(headers, dict) and bool(headers)


def _cred_vars(server: Any) -> list[str]:
    """Names of the credential environment variable(s) a server needs.

    For a stdio server the ``env`` map keys ARE the credential var names (e.g.
    ``GITHUB_PERSONAL_ACCESS_TOKEN``) and are preferred. For a remote server the
    credential lives in a ``${VAR}`` placeholder inside a ``headers`` value (e.g.
    ``"Authorization": "Bearer ${GITHUB_TOKEN}"`` -> ``GITHUB_TOKEN``). Returns an
    empty list when no concrete var can be extracted (e.g. an authed-by-heuristic
    header with a literal token), so the advisory falls back to a generic name
    rather than emitting a wrong concrete one.
    """

    if not isinstance(server, dict):
        return []
    env = server.get("env")
    if isinstance(env, dict) and env:
        return [str(key) for key in env]
    found: list[str] = []
    headers = server.get("headers")
    if isinstance(headers, dict) and headers:
        for value in headers.values():
            if isinstance(value, str):
                for var in _HEADER_VAR_RE.findall(value):
                    if var not in found:
                        found.append(var)
    # A remote server often carries no headers at all and instead parameterises
    # the endpoint itself -- `"url": "${GRAFANA_MCP_URL}"`. That var is what the
    # operator must forward, so name it rather than falling back to a generic
    # placeholder. Strips any `:-default` shell-style suffix.
    url = server.get("url")
    if isinstance(url, str):
        for var in _HEADER_VAR_RE.findall(url):
            var = var.split(":-", 1)[0].strip()
            if var and var not in found:
                found.append(var)
    return found


def _read_connectors(root: Path) -> dict[str, ConnectorSpec]:
    """The bundle's declared connectors, or ``{}`` when there is nothing usable.

    Deliberately fail-soft, the same trade ``curie_runner.connectors._read``
    makes: this module is a *diagnostic*, and crashing it removes the only tool
    the author has for "my tools are missing". An absent, unreadable, or
    non-validating ``connectors.yaml`` therefore contributes zero rows rather
    than raising -- and it is not a swallow, because ``validate_bundle`` has
    already reported that same file as ``invalid_bundle`` on the real path.

    Parsed through ``safe_load_unique`` + ``validate_connectors``, the same pair
    ``connectors.derive_mcp_servers`` and
    ``plugin_format.approval_policy.connector_server_names`` use -- and it
    accepts exactly what they accept, including their fail-closed answer, so the
    census cannot disagree with the mount about what the file says.
    """

    path = root / CONNECTORS_FILE
    if not path.is_file():
        return {}
    try:
        data = safe_load_unique(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, yaml.YAMLError):
        return {}
    parsed, _errors = validate_connectors(data)
    if parsed is None:
        # `validate_connectors` returns `(parsed if not errors else None)`, so a
        # `None` here means it refused the WHOLE file -- a `sealed_secrets` block
        # with no decrypt path, a reserved name, a bad build platform. Refuse it
        # too. Re-validating the survivors individually would make this census
        # the ONE reader of the file that still reports rows where
        # `approval_policy.connector_server_names` returns `None` and
        # `connectors._read` mounts nothing, and a census that disagrees with the
        # mount is worse than one that says nothing. `validate_bundle` reports
        # the refusal itself on the real path, as `invalid_bundle`.
        return {}
    # Reached only when `validate_connectors` produced NO errors, so these rows
    # are the fully valid file, byte-for-byte the same set the mount derives
    # from and the approval policy names.
    return dict(parsed.connectors)


def _connector_form(spec: ConnectorSpec) -> str:
    """Which of the three tier-relevant shapes a declared connector has.

    ``remote`` (``url:``) is already running and reachable from anywhere, so its
    absence is a real defect. ``hosted`` (``image:`` or ADR-0113's ``build:``,
    which changes only where the image comes FROM) needs Curie to run it, which
    this tier does not. ``hosted_unhosted`` is the same hosted connector that
    also said where to reach a copy the author already runs (#1160) -- the one
    hosted shape this tier can genuinely exercise, so it must stay able to pass.

    ``url`` is tested FIRST and that order is load-bearing.
    ``validate_connectors`` decides which form is present by TRUTHINESS
    (``bool(spec.image)`` / ``bool(spec.build)`` / ``bool(spec.url)``), while
    ``ConnectorSpec.is_hosted`` is ``image is not None or build is not None``.
    So ``image: ""`` plus a ``url:`` validates with zero errors and reaches here
    with BOTH attributes set and ``is_hosted`` true. Branching on ``is_hosted``
    first would call that spec hosted and cost it the remote advisory its red
    actually needs. ``is_hosted`` is still used for the hosted arm, once ``url``
    has been ruled out.

    Follow-up: this tier-reachability classification duplicates knowledge
    ``ConnectorSpec`` already carries in raw form (``is_hosted``,
    ``unhosted_url``), and lives here only because ``plugin-format`` is a frozen
    package. It should fold into ``ConnectorSpec`` when that package unfreezes.
    """

    if spec.url:
        return "remote"
    return "hosted_unhosted" if spec.is_hosted and spec.unhosted_url else "hosted"


def _connector_authed(spec: ConnectorSpec) -> bool:
    """True when the connector carries a credential in any of its four holders.

    Everything ``secret_names()`` aggregates (``secrets``, whether
    Curie-resolved or a ``SecretRef``, plus ``sealed_secrets`` and
    ``secret_files``) and a remote connector's ``headers`` all mean the same
    thing here: something the credential-free offline check never forwarded. A
    connector carrying none of them must stay False, or the authed advisory
    fires on every connector and stops carrying information.
    """

    return bool(spec.secret_names() or spec.headers)


def _connector_cred_vars(spec: ConnectorSpec) -> list[str]:
    """The credential env-var NAMES ``--secret`` would have to forward.

    NAMES only, never values -- ADR-0090 is unchanged by this module. In
    particular a ``SecretRef`` contributes its ``name``, never its
    ``from_secret`` (that is the Kubernetes Secret Curie points at and
    forwarding it would be a wrong concrete answer).

    Built on ``ConnectorSpec.secret_names()``, which already aggregates exactly
    those holders in exactly that shape (a bare ``secrets:`` entry, a
    ``SecretRef``'s ``name``, and the ``sealed_secrets`` / ``secret_files``
    KEYS). Only the keys of those two maps: a ``secret_files`` VALUE is a file
    path and a ``sealed_secrets`` VALUE is a sealed blob, and neither is ever
    emitted (ADR-0090). Reusing the helper keeps this census from drifting from
    the way every other reader of the spec names the same credentials.

    What ``secret_names()`` does NOT cover, and this adds: the ``${VAR}``
    placeholders in ``url`` / ``unhosted_url`` / ``headers``, which are the
    remote and unhosted forms' way of naming the same thing.

    Order-stable and deduped so the advisory reads the way the file does.
    """

    found: list[str] = []

    def _push(var: str) -> None:
        if var and var not in found:
            found.append(var)

    for name in spec.secret_names():
        _push(str(name))
    for value in (spec.url, spec.unhosted_url, *spec.headers.values()):
        if isinstance(value, str):
            for var in _HEADER_VAR_RE.findall(value):
                _push(var.split(":-", 1)[0].strip())
    return found


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


# --------------------------------------------------------------------------- #
# Verdict (pure: no I/O, no SDK)
# --------------------------------------------------------------------------- #
def evaluate(declared: list[dict[str, Any]], registered: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute the verdict from declared intent vs registered MCP servers.

    The registered list is ``McpServerStatus``-shaped. Only the bundle's **own**
    servers (``scope == "dynamic"`` or a ``plugin:``-prefixed name when scope is
    absent) affect the verdict; ambient project/user servers never do. The rule is
    declared-anchored: one match row per declared server, green iff every declared
    server matched a connected own-server with at least one tool (plan Section 3).
    """

    own = [s for s in registered if _plugin_owned(s)]
    matches: list[dict[str, Any]] = []
    reasons: list[str] = []
    hints: list[str] = []

    if any(d.get("form") == "string_pointer" for d in declared):
        hints.append(_STRING_POINTER_HINT)

    # An authed server needs a credential the credential-free offline check never
    # forwards, so a green here proves only wiring and a red may be just a missing
    # token. Advise regardless of verdict so a demo-watcher cannot misread either.
    for row in declared:
        cred_vars = list(row.get("cred_vars") or [])
        if row.get("source") == CONNECTORS_FILE and not row.get("remote"):
            # A hosted connector's red is a tier fact, not a bundle defect, and
            # the generic authed advisory ("run `curie skill up --secret ...`")
            # would send the author at a command that cannot host it either.
            # A REMOTE connector falls through: it is unreachable for the same
            # `--network none` reason as any other remote server (#1093), and
            # the existing advisory already says exactly that.
            hints.append(_connector_advisory(str(row["name"]), str(row.get("form")), cred_vars))
        elif row.get("remote"):
            # Takes precedence over the authed advisory: unreachable-by-design is
            # the more specific and more actionable explanation of the red.
            hints.append(_remote_advisory(str(row["name"]), cred_vars))
        elif row.get("authed"):
            hints.append(_authed_advisory(str(row["name"]), cred_vars))

    connected_with_tools = 0
    for row in declared:
        name = str(row["name"])
        match = _find_match(name, own)
        tool_count = len(match.get("tools") or []) if match is not None else 0
        connected = match is not None and match.get("status") == "connected" and tool_count >= 1
        if connected:
            connected_with_tools += 1
        else:
            reasons.append(_reason_for(name, match, row))
        matches.append(
            {
                "declared": name,
                "registered": match["name"] if match is not None else None,
                "connected": connected,
                "tool_count": tool_count,
            }
        )

    if declared and connected_with_tools == 0:
        reasons.append(f"declared {len(declared)} MCP server(s); none registered with tools")

    verdict = "red" if reasons else "green"
    return {"matches": matches, "verdict": verdict, "reasons": reasons, "hints": hints}


def _plugin_owned(status: dict[str, Any]) -> bool:
    scope = status.get("scope")
    if scope == "dynamic":
        return True
    return scope is None and str(status.get("name", "")).startswith("plugin:")


def _find_match(name: str, own: list[dict[str, Any]]) -> dict[str, Any] | None:
    for server in own:
        registered_name = str(server.get("name", ""))
        if registered_name == name or registered_name.endswith(":" + name):
            return server
    return None


def _reason_for(name: str, match: dict[str, Any] | None, row: dict[str, Any]) -> str:
    if match is None:
        # A purely hosted connector was never mounted here because there is no
        # address to mount -- `derive_mcp_servers` logs that same set as
        # "declared but not exercisable in this tier", and this reason has to
        # agree with that log rather than contradict it with a bare "never
        # registered", which reads as a bundle defect (#2348).
        if row.get("source") == CONNECTORS_FILE and row.get("form") == "hosted":
            return (
                f"declared connector {name} was not exercisable in this tier: "
                "Curie hosts it and this check hosts no connector, so it was "
                "never mounted"
            )
        return f"declared {name} never registered"
    status = match.get("status")
    if status == "failed":
        error = match.get("error")
        return str(error) if error else f"declared {name} failed to connect"
    if status == "pending":
        return f"declared {name} did not finish initializing"
    if status == "needs-auth":
        return (
            f"declared {name} needs authentication; the offline check is "
            "credential-free and cannot validate a credential-gated server"
        )
    if status == "connected":
        return f"declared {name} connected with zero tools"
    return f"declared {name} registered with status {status!r} and no tools"


# --------------------------------------------------------------------------- #
# Real-loader run
# --------------------------------------------------------------------------- #
async def run_check(plugin_dir: str) -> dict[str, Any]:
    """Validate, connect, poll, and assemble the full Section-3 result dict."""

    try:
        plugins = load_plugins(plugin_dir)
    except PluginBundleError as exc:
        return _invalid_bundle_result(plugin_dir, [str(exc)])

    declared = extract_declared(plugin_dir)
    timeout_s = _timeout_s()

    try:
        registered = await asyncio.wait_for(_connect_and_poll(plugins, plugin_dir), timeout_s)
    except TimeoutError:
        return _red_result(plugin_dir, declared, f"MCP init did not complete within {timeout_s}s")
    except Exception as exc:
        # A non-timeout failure while setting up the MCP client (Claude CLI
        # subprocess fails to start, an incompatible --image, an SDK error)
        # would otherwise escape main() before any JSON is printed, and the CLI
        # would report an opaque contract violation. Surface it as a valid RED
        # result naming the failure instead of swallowing it. A genuinely
        # malformed bundle is still invalid_bundle (caught above), not red.
        return _red_result(
            plugin_dir,
            declared,
            f"MCP client failed to start: {type(exc).__name__}: {exc}",
        )

    return _assemble(plugin_dir, declared, registered, evaluate(declared, registered))


def _red_result(plugin_dir: str, declared: list[dict[str, Any]], reason: str) -> dict[str, Any]:
    """Assemble a RED result (no registered servers) with an override reason FIRST.

    Shared by the timeout and MCP-client-startup failure paths in ``run_check``:
    both assemble the base result from empty registered servers, then force the
    verdict red with their own reason string.

    The override is PREPENDED, not substituted: it is the proximate cause and so
    belongs first, but the per-declared-server reasons ``evaluate`` computed
    ("declared connector X was not exercisable in this tier") are the ones #2348
    is about, and replacing the list threw them away -- the author saw only a
    generic timeout. De-duplicated in case ``evaluate`` already said the same
    thing.
    """

    result = _assemble(plugin_dir, declared, [], evaluate(declared, []))
    result["verdict"] = "red"
    existing = [text for text in result["reasons"] if text != reason]
    result["reasons"] = [reason, *existing]
    return result


async def _connect_and_poll(plugins: list[Any], plugin_dir: str) -> list[dict[str, Any]]:
    """Connect a real client and poll get_mcp_status until own servers settle.

    Runs no query. Returns the verbatim ``mcpServers`` list once no plugin-owned
    server is still ``pending`` (ambient servers included for transparency). The
    caller wraps this in ``asyncio.wait_for``; ``disconnect()`` runs on every path.

    The bundle's declared connectors are mounted alongside the plugins so the
    check actually TRIES them. There is no connector scope here (no release, no
    agent, no namespace -- this tier hosts nothing), so ``derive_mcp_servers``
    yields exactly the reachable fallback set: a remote connector's own ``url``
    and a hosted connector's ``unhosted_url`` (#1160), and nothing for a purely
    hosted one. Inventing an address for that last case would turn a clear "not
    exercisable" into a mid-turn connection refused.
    """

    options = build_options(
        plugins=plugins,
        mcp_servers=derive_mcp_servers(plugin_dir, release=None, agent=None, namespace=None),
        model=None,
        system_prompt=None,
        max_turns=1,
        max_budget_usd=None,
        resume=None,
    )
    client = ClaudeSDKClient(options)
    await client.connect()
    try:
        while True:
            status = await client.get_mcp_status()
            servers = [dict(s) for s in status.get("mcpServers", [])]
            pending = [s for s in servers if _plugin_owned(s) and s.get("status") == "pending"]
            if not pending:
                return servers
            await asyncio.sleep(_POLL_INTERVAL_S)
    finally:
        await client.disconnect()


def _assemble(
    plugin_dir: str,
    declared: list[dict[str, Any]],
    registered: list[dict[str, Any]],
    verdict: dict[str, Any],
) -> dict[str, Any]:
    return {
        "check": CHECK_NAME,
        "version": CHECK_VERSION,
        "plugin_dir": plugin_dir,
        "declared": declared,
        "registered": registered,
        "matches": verdict["matches"],
        "verdict": verdict["verdict"],
        "reasons": verdict["reasons"],
        "hints": verdict["hints"],
    }


def _invalid_bundle_result(plugin_dir: str, reasons: list[str]) -> dict[str, Any]:
    return {
        "check": CHECK_NAME,
        "version": CHECK_VERSION,
        "plugin_dir": plugin_dir,
        "declared": [],
        "registered": [],
        "matches": [],
        "verdict": "invalid_bundle",
        "reasons": reasons,
        "hints": [],
    }


def _timeout_s() -> int:
    raw = os.environ.get("CURIE_CHECK_TIMEOUT_S")
    if not raw:
        return _DEFAULT_TIMEOUT_S
    try:
        value = int(raw)
    except ValueError:
        return _DEFAULT_TIMEOUT_S
    return value if value > 0 else _DEFAULT_TIMEOUT_S


# --------------------------------------------------------------------------- #
# Module entry
# --------------------------------------------------------------------------- #
def main() -> int:
    logging.basicConfig(level=logging.INFO, stream=sys.stderr)
    plugin_dir = os.environ.get(PLUGIN_DIR_ENV)
    if not plugin_dir or not Path(plugin_dir).is_dir():
        result = _invalid_bundle_result(plugin_dir or "", [f"plugin dir not found: {plugin_dir!r}"])
        print(json.dumps(result))
        return _EXIT_CODES["invalid_bundle"]

    result = asyncio.run(run_check(plugin_dir))
    print(json.dumps(result))
    return _EXIT_CODES.get(str(result["verdict"]), 1)


if __name__ == "__main__":
    sys.exit(main())
