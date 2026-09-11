"""Tests for the offline MCP load check (`curie_runner.check`, issue #337).

Test-first: these pin the frozen runner<->CLI JSON seam (plan Section 3) and the
verdict rules. Until ``check.py`` exists the module import fails collection --
that is the intended red for the Stage-2 test-writer pass.

Mocking discipline (plan Section 5): nothing here is mocked. Tests 1-3 are pure
functions over real fixture dirs and literal ``McpServerStatus``-shaped dicts;
test 4 runs the module as a subprocess; test 5 drives the *real* Claude Code
loader (gated on the ``claude`` CLI being present).
"""

import json
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import anyio
import pytest
from curie_runner.check import evaluate, extract_declared, run_check

_HERE = Path(__file__).resolve().parent
_FIXTURES = _HERE / "fixtures"
_MCP_GREEN = _FIXTURES / "mcp_green"
_MCP_RED_POINTER = _FIXTURES / "mcp_red_pointer"
_MCP_RED_BROKEN = _FIXTURES / "mcp_red_broken"
_PLUGIN_FORMAT_FIXTURES = _HERE.parents[1] / "packages/plugin-format/tests/fixtures"

_CLAUDE_ON_PATH = shutil.which("claude") is not None

# The github-issues example forwards this credential env var. Held as a named
# constant (not an inline "<NAME>": "<placeholder>" literal pair) so the
# secret-scan pre-commit hook does not false-positive on the access-token-shaped
# placeholder; the value is a ${VAR} interpolation reference, never a real token.
_GH_TOKEN_ENV = "GITHUB_PERSONAL_ACCESS_TOKEN"
_GH_TOKEN_PLACEHOLDER = "${" + _GH_TOKEN_ENV + "}"


# --------------------------------------------------------------------------- #
# helpers
# --------------------------------------------------------------------------- #
def _declared(
    name: str,
    source: str = "plugin.json",
    form: str = "inline",
    *,
    authed: bool = False,
    cred_vars: list[str] | None = None,
    remote: bool = False,
) -> dict:
    return {
        "name": name,
        "source": source,
        "form": form,
        "authed": authed,
        "cred_vars": list(cred_vars or []),
        "remote": remote,
    }


def _tool(name: str) -> dict:
    return {"name": name}


def _status(
    name: str,
    *,
    status: str = "connected",
    scope: str | None = "dynamic",
    tools: list[dict] | None = None,
    error: str | None = None,
) -> dict:
    entry: dict = {"name": name, "status": status, "tools": list(tools or [])}
    if scope is not None:
        entry["scope"] = scope
    if error is not None:
        entry["error"] = error
    return entry


def _write_bundle(root: Path, manifest: dict, *, mcp_files: dict[str, dict] | None = None) -> Path:
    (root / ".claude-plugin").mkdir(parents=True, exist_ok=True)
    (root / ".claude-plugin" / "plugin.json").write_text(json.dumps(manifest), encoding="utf-8")
    for rel, payload in (mcp_files or {}).items():
        target = root / rel
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(json.dumps(payload), encoding="utf-8")
    return root


# --------------------------------------------------------------------------- #
# Test 1 -- declared extraction over real bundle dirs (no mocks)
# --------------------------------------------------------------------------- #
def test_extract_inline_object_form() -> None:
    # Canonical declared shape: exactly {name, source, form, authed, cred_vars,
    # remote},
    # nothing else. A plain stdio server with no env is authed=False, cred_vars=[].
    assert extract_declared(str(_MCP_GREEN)) == [
        {
            "name": "green-probe",
            "source": "plugin.json",
            "form": "inline",
            "authed": False,
            "cred_vars": [],
            "remote": False,
        }
    ]


def test_extract_string_pointer_form() -> None:
    declared = extract_declared(str(_MCP_RED_POINTER))
    by_name = {d["name"]: d for d in declared}
    assert "pointer-probe" in by_name
    assert by_name["pointer-probe"]["form"] == "string_pointer"
    assert by_name["pointer-probe"]["source"] == "plugin.json"


def test_extract_string_pointer_missing_file_is_red_path(tmp_path: Path) -> None:
    # Intent still surfaces (a string-pointer declaration is visible) even though
    # the pointed file does not exist; and the missing target drives a red verdict.
    bundle = _write_bundle(
        tmp_path / "missing_ptr", {"name": "missing-ptr", "mcpServers": "config/nope.json"}
    )
    declared = extract_declared(str(bundle))
    assert declared, "a string-pointer declaration must still surface as a declared entry"
    assert all(d["form"] == "string_pointer" for d in declared)
    result = evaluate(declared, [])
    assert result["verdict"] == "red"
    assert result["reasons"]


def test_extract_bare_mcp_json_form(tmp_path: Path) -> None:
    bundle = _write_bundle(
        tmp_path / "bare",
        {"name": "bare-bundle"},
        mcp_files={".mcp.json": {"mcpServers": {"bare-probe": {"command": "python3"}}}},
    )
    declared = extract_declared(str(bundle))
    by_name = {d["name"]: d for d in declared}
    assert "bare-probe" in by_name
    assert by_name["bare-probe"]["form"] == "bare_file"
    assert by_name["bare-probe"]["source"] == ".mcp.json"


def test_extract_root_plugin_json_fallback_is_reported(tmp_path: Path) -> None:
    # Root-level plugin.json (no .claude-plugin/ dir) is the accepted fallback
    # location (packages/plugin-format's MANIFEST_LOCATIONS, issue #653); the
    # resolver must find it and the declared server must still surface.
    bundle = tmp_path / "root_manifest"
    bundle.mkdir()
    (bundle / "plugin.json").write_text(
        json.dumps({"name": "root-bundle", "mcpServers": {"root-probe": {"command": "python3"}}}),
        encoding="utf-8",
    )
    declared = extract_declared(str(bundle))
    by_name = {d["name"]: d for d in declared}
    assert "root-probe" in by_name
    assert by_name["root-probe"]["form"] == "inline"
    assert by_name["root-probe"]["source"] == "plugin.json"


def test_extract_no_mcp_anywhere_is_empty(tmp_path: Path) -> None:
    bundle = _write_bundle(tmp_path / "nomcp", {"name": "no-mcp-bundle"})
    assert extract_declared(str(bundle)) == []


# --------------------------------------------------------------------------- #
# Test 1b -- authed flag: a server carrying a credential (env/headers) is marked
# so the report can say it was NOT exercised by the credential-free offline check.
# --------------------------------------------------------------------------- #
def test_extract_authed_inline_env_marks_only_the_credentialed_server(
    tmp_path: Path,
) -> None:
    # Inline plugin.json form: a non-empty `env` map is the authed signal; a plain
    # stdio server with no env is authed=False.
    bundle = _write_bundle(
        tmp_path / "authed_inline",
        {
            "name": "authed-inline",
            "mcpServers": {
                "github": {
                    "command": "mcp-server-github",
                    "args": [],
                    "env": {_GH_TOKEN_ENV: _GH_TOKEN_PLACEHOLDER},
                },
                "plain": {"command": "python3", "args": []},
            },
        },
    )
    by_name = {d["name"]: d for d in extract_declared(str(bundle))}
    assert by_name["github"]["authed"] is True
    # cred_vars names the real env-var to forward via --secret, not the server name.
    assert by_name["github"]["cred_vars"] == ["GITHUB_PERSONAL_ACCESS_TOKEN"]
    assert by_name["plain"]["authed"] is False
    assert by_name["plain"]["cred_vars"] == []


def test_extract_authed_bare_mcp_json_env_marks_server(tmp_path: Path) -> None:
    # Bare .mcp.json form (the real github-issues example shape): the env block
    # with a ${VAR} value is the authed signal.
    bundle = _write_bundle(
        tmp_path / "authed_bare",
        {"name": "authed-bare"},
        mcp_files={
            ".mcp.json": {
                "mcpServers": {
                    "github": {
                        "command": "mcp-server-github",
                        "env": {_GH_TOKEN_ENV: _GH_TOKEN_PLACEHOLDER},
                    },
                    "plain": {"command": "python3"},
                }
            }
        },
    )
    by_name = {d["name"]: d for d in extract_declared(str(bundle))}
    assert by_name["github"]["authed"] is True
    assert by_name["github"]["cred_vars"] == ["GITHUB_PERSONAL_ACCESS_TOKEN"]
    assert by_name["plain"]["authed"] is False
    assert by_name["plain"]["cred_vars"] == []


def test_extract_empty_env_is_not_authed(tmp_path: Path) -> None:
    # The signal is a NON-EMPTY env map; an empty env block does not carry a
    # credential and must stay authed=False.
    bundle = _write_bundle(
        tmp_path / "empty_env",
        {"name": "empty-env", "mcpServers": {"noenv": {"command": "python3", "env": {}}}},
    )
    by_name = {d["name"]: d for d in extract_declared(str(bundle))}
    assert by_name["noenv"]["authed"] is False


def test_extract_authed_remote_headers_marks_server(tmp_path: Path) -> None:
    # For a REMOTE server, an Authorization `headers` map is the authed signal.
    bundle = _write_bundle(
        tmp_path / "authed_remote",
        {
            "name": "authed-remote",
            "mcpServers": {
                "remote": {
                    "type": "http",
                    "url": "https://example.com/mcp",
                    "headers": {"Authorization": "Bearer ${REMOTE_TOKEN}"},
                }
            },
        },
    )
    by_name = {d["name"]: d for d in extract_declared(str(bundle))}
    assert by_name["remote"]["authed"] is True
    # The ${VAR} placeholder inside the header value is the credential var name.
    assert by_name["remote"]["cred_vars"] == ["REMOTE_TOKEN"]


# --------------------------------------------------------------------------- #
# Test 2 -- verdict pure function over literal McpServerStatus-shaped dicts
# --------------------------------------------------------------------------- #
def test_verdict_zero_declared_is_green() -> None:
    result = evaluate([], [_status("plugin:x:y", tools=[_tool("t")])])
    assert result["verdict"] == "green"
    assert result["reasons"] == []


def test_verdict_declared_none_registered_is_red() -> None:
    result = evaluate([_declared("a")], [])
    assert result["verdict"] == "red"
    assert result["reasons"]


def test_verdict_connected_with_zero_tools_is_red() -> None:
    result = evaluate([_declared("a")], [_status("plugin:x:a", tools=[])])
    assert result["verdict"] == "red"
    assert result["reasons"]


def test_verdict_failed_propagates_error_into_reasons() -> None:
    result = evaluate(
        [_declared("a")],
        [_status("plugin:x:a", status="failed", tools=[], error="spawn ENOENT: bad-command")],
    )
    assert result["verdict"] == "red"
    assert any("spawn ENOENT" in r for r in result["reasons"])


def test_verdict_needs_auth_is_red() -> None:
    result = evaluate([_declared("a")], [_status("plugin:x:a", status="needs-auth", tools=[])])
    assert result["verdict"] == "red"
    assert result["reasons"]


def test_verdict_pending_at_deadline_is_red() -> None:
    result = evaluate([_declared("a")], [_status("plugin:x:a", status="pending", tools=[])])
    assert result["verdict"] == "red"
    assert result["reasons"]


def test_verdict_scoped_dynamic_name_matches_declared() -> None:
    # plugin:<bundle>:<server> with scope "dynamic" matches declared "probe" by
    # the :-segment suffix rule.
    result = evaluate(
        [_declared("probe")],
        [_status("plugin:mybundle:probe", scope="dynamic", tools=[_tool("word_count")])],
    )
    assert result["verdict"] == "green"
    assert result["reasons"] == []


def test_verdict_scope_missing_name_prefix_fallback_is_green() -> None:
    # scope key intentionally absent -> plugin_owned via the "plugin:" name-prefix
    # fallback (Section 3 rule 1), so the server still counts and the verdict is green.
    own = _status("plugin:mybundle:probe", scope=None, tools=[_tool("word_count")])
    assert "scope" not in own
    result = evaluate([_declared("probe")], [own])
    assert result["verdict"] == "green"


def test_verdict_partial_registration_is_red() -> None:
    # Two declared, only one connected-with-tools -> red (partial coverage).
    result = evaluate(
        [_declared("a"), _declared("b")],
        [_status("plugin:x:a", tools=[_tool("t")])],
    )
    assert result["verdict"] == "red"
    assert result["reasons"]


def test_verdict_unmatched_declared_with_ambient_connected_is_red() -> None:
    # F2 hole (load-bearing): a declared own-server that never registered must go
    # RED even when the host has an AMBIENT (scope project/user) connected-with-tools
    # server present. Deletion check: without the own-server filter + declared-
    # anchoring, a globally-aggregate "is anything connected with tools?" rule would
    # count the ambient server and FALSE-GREEN this exact shape -- the #336 slip.
    #
    # The ambient server's NAME collides with the declared server by the
    # :-segment-suffix rule ("something:probe" ends with ":probe"), so declared-
    # anchoring ALONE is not enough: _find_match would match it. Only the scope
    # own-filter (scope=="project" is not plugin-owned) excludes it and keeps this
    # RED. Deletion check: remove the scope own-filter and this test false-greens.
    ambient = _status("something:probe", scope="project", tools=[_tool("ambient_tool")])
    result = evaluate([_declared("probe")], [ambient])
    assert result["verdict"] == "red"
    assert result["reasons"]


def test_verdict_ambient_server_never_flips_green() -> None:
    own = _status("plugin:x:a", scope="dynamic", tools=[_tool("t")])
    ambient = _status("proj-server", scope="user", tools=[_tool("ambient_tool")])
    result = evaluate([_declared("a")], [own, ambient])
    assert result["verdict"] == "green"


def test_verdict_ambient_server_never_rescues_red() -> None:
    failed_own = _status("plugin:x:a", status="failed", tools=[], error="boom")
    ambient = _status("proj-server", scope="project", tools=[_tool("ambient_tool")])
    result = evaluate([_declared("a")], [failed_own, ambient])
    assert result["verdict"] == "red"


# --------------------------------------------------------------------------- #
# Test 3 -- string-pointer fingerprint (hints, not reasons) + reasons invariant
# --------------------------------------------------------------------------- #
def test_string_pointer_fingerprint_lives_in_hints_not_reasons() -> None:
    # The real loader silently ignores the string-pointer form, so nothing
    # registers; the diagnostic fingerprint must surface in `hints`.
    # Deletion check: remove the hint emission and this fails.
    declared = extract_declared(str(_MCP_RED_POINTER))
    result = evaluate(declared, [])
    assert result["verdict"] == "red"
    assert any("string pointer" in h.lower() for h in result["hints"])
    assert not any("string pointer" in r.lower() for r in result["reasons"])


def test_reasons_nonempty_iff_verdict_not_green_and_green_may_carry_hints() -> None:
    green = evaluate([], [])
    assert green["verdict"] == "green"
    assert green["reasons"] == []

    red = evaluate([_declared("a")], [])
    assert red["verdict"] == "red"
    assert red["reasons"]

    # E8 bare-file-rescued shape: declared as a string pointer but registered
    # anyway (a coexisting bare .mcp.json). Verdict is GREEN, reasons empty, yet
    # the advisory string-pointer fingerprint still appears in hints.
    rescued = evaluate(
        [_declared("probe", form="string_pointer")],
        [_status("plugin:x:probe", scope="dynamic", tools=[_tool("t")])],
    )
    assert rescued["verdict"] == "green"
    assert rescued["reasons"] == []
    assert any("string pointer" in h.lower() for h in rescued["hints"])


# --------------------------------------------------------------------------- #
# Test 3b -- authed-server advisory always lands in hints (green AND red), so a
# demo-watcher can't misread a credential-free result. The offline check runs
# --network none and forwards no secret: a green proves only wiring (tool-list
# needs no auth) and a red may mean only "no token", not "broken".
# --------------------------------------------------------------------------- #
_AUTHED_ADVISORY = "not exercised offline"


def _authed_hint(result: dict) -> str:
    return next(h for h in result["hints"] if _AUTHED_ADVISORY in h)


def test_authed_advisory_in_hints_when_registered_green() -> None:
    # Authed server connected with tools -> verdict green, yet the advisory must
    # still fire so the green is not read as "auth verified".
    declared = [_declared("github", authed=True, cred_vars=["GITHUB_PERSONAL_ACCESS_TOKEN"])]
    result = evaluate(declared, [_status("plugin:x:github", tools=[_tool("list_issues")])])
    assert result["verdict"] == "green"
    assert result["reasons"] == []
    advisory = _authed_hint(result)
    assert "github" in advisory
    # --secret forwards an env-var NAME, so the advisory must name the real
    # credential var, NOT the server name (following `--secret github` leaves the
    # token absent and the advertised end-to-end test fails).
    assert "--secret GITHUB_PERSONAL_ACCESS_TOKEN" in advisory
    assert "--secret github" not in advisory


def test_authed_advisory_in_hints_when_absent_red() -> None:
    # Authed server never registered -> verdict red; the advisory still fires so
    # the red is not necessarily read as "broken" (may just be "no token").
    declared = [_declared("github", authed=True, cred_vars=["GITHUB_PERSONAL_ACCESS_TOKEN"])]
    result = evaluate(declared, [])
    assert result["verdict"] == "red"
    advisory = _authed_hint(result)
    assert "github" in advisory
    assert "--secret GITHUB_PERSONAL_ACCESS_TOKEN" in advisory
    assert "--secret github" not in advisory


def test_authed_advisory_names_header_var() -> None:
    # A remote server's credential var comes from the ${VAR} header placeholder.
    declared = [_declared("remote", authed=True, cred_vars=["REMOTE_TOKEN"])]
    advisory = _authed_hint(evaluate(declared, []))
    assert "--secret REMOTE_TOKEN" in advisory


def test_authed_advisory_names_multiple_vars() -> None:
    declared = [_declared("multi", authed=True, cred_vars=["VAR_ONE", "VAR_TWO"])]
    advisory = _authed_hint(evaluate(declared, []))
    assert "--secret VAR_ONE --secret VAR_TWO" in advisory


def test_authed_advisory_falls_back_to_generic_when_no_cred_var() -> None:
    # Authed by heuristic but no extractable var (e.g. a literal header token):
    # fall back to the generic placeholder rather than emit a wrong concrete name.
    declared = [_declared("mystery", authed=True)]
    advisory = _authed_hint(evaluate(declared, []))
    assert "--secret <NAME>" in advisory


def test_non_authed_server_gets_no_offline_advisory() -> None:
    # Deletion check: the advisory is gated on authed=True. A plain server that
    # needs no credential must NOT carry the "not exercised offline" hint.
    declared = [_declared("plain", authed=False)]
    result = evaluate(declared, [_status("plugin:x:plain", tools=[_tool("t")])])
    assert not any(_AUTHED_ADVISORY in h for h in result["hints"]), result["hints"]


# --------------------------------------------------------------------------- #
# Test 4 -- JSON purity + exit codes (module run as a subprocess)
# --------------------------------------------------------------------------- #
def _run_module(
    plugin_dir: str, env_extra: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    env = {**os.environ, "CURIE_PLUGIN_DIR": plugin_dir}
    if env_extra:
        env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "curie_runner.check"],
        capture_output=True,
        text=True,
        env=env,
        timeout=120,
    )


def test_module_nonexistent_dir_exits_2_with_invalid_bundle_json(tmp_path: Path) -> None:
    proc = _run_module(str(tmp_path / "does-not-exist"), env_extra={"HOME": str(tmp_path)})
    assert proc.returncode == 2
    # stdout must be exactly ONE json.loads-able document (stderr may carry logs).
    doc = json.loads(proc.stdout)
    assert doc["verdict"] == "invalid_bundle"
    assert doc["reasons"]


def test_module_invalid_bundle_exits_2_with_json(tmp_path: Path) -> None:
    bad = _PLUGIN_FORMAT_FIXTURES / "bad_manifest_name"
    proc = _run_module(str(bad), env_extra={"HOME": str(tmp_path)})
    assert proc.returncode == 2
    doc = json.loads(proc.stdout)
    assert doc["verdict"] == "invalid_bundle"
    assert doc["reasons"]


# --------------------------------------------------------------------------- #
# Test 5 -- real-loader integration (gated on the real `claude` CLI, isolated HOME)
# --------------------------------------------------------------------------- #
@pytest.mark.skipif(
    not _CLAUDE_ON_PATH,
    reason="requires the real `claude` CLI on PATH to spawn MCP servers",
)
def test_run_check_green_bundle_registers_tools(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Isolated HOME (plan Section 5, E7): the CLI caches a failed MCP connection
    # ~15 min so consecutive host runs go flaky, and an ambient HOME leaks
    # project/user MCP servers into the status; a clean HOME leaves only the
    # bundle's own servers. No credential env is set (spike: connect() is
    # credential-free and there is no query() in the code path).
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    result = anyio.run(run_check, str(_MCP_GREEN))
    assert result["verdict"] == "green", result
    assert result["reasons"] == []


def test_run_check_red_pointer_bundle_is_invalid_bundle() -> None:
    # The `.mcp.json` string-pointer form is rejected statically by `load_plugins`
    # at run_check's step 1, so this never reaches `red`: a genuinely malformed
    # bundle is `invalid_bundle`. Detection is static -- no container boot and no
    # MCP server spawn -- so this needs no real `claude` CLI and stays unskipped.
    result = anyio.run(run_check, str(_MCP_RED_POINTER))
    assert result["verdict"] == "invalid_bundle", result
    assert result["reasons"], result
    joined = " ".join(result["reasons"])
    # Prove it was rejected for THIS reason (the pointer form), not some other
    # bundle error: the stable diagnostic code plus the offending path.
    assert "[mcp.declared_pointer]" in joined, joined
    assert "config/mcp.json" in joined, joined


# --------------------------------------------------------------------------- #
# Remote servers: unreachable by design, not a bundle defect (#1093)
# --------------------------------------------------------------------------- #
def test_remote_server_declared_by_url_only_is_marked_remote(tmp_path: Path) -> None:
    # The common remote shape carries neither `env` nor `headers` -- the endpoint
    # itself is the parameterised part -- so it was invisible to both the authed
    # flag and cred-var extraction, and produced a bare red with no explanation.
    root = _write_bundle(
        tmp_path,
        {"name": "b", "version": "0.1.0"},
        mcp_files={
            ".mcp.json": {"mcpServers": {"grafana": {"type": "http", "url": "${GRAFANA_MCP_URL}"}}}
        },
    )
    rows = extract_declared(str(root))
    assert rows == [
        _declared(
            "grafana",
            source=".mcp.json",
            form="bare_file",
            cred_vars=["GRAFANA_MCP_URL"],
            remote=True,
        )
    ]


def test_remote_url_var_strips_a_shell_style_default(tmp_path: Path) -> None:
    # `${VAR:-fallback}` is how a bundle keeps one declaration working across
    # tiers; the operator still forwards VAR, so name VAR and not the default.
    root = _write_bundle(
        tmp_path,
        {"name": "b", "version": "0.1.0"},
        mcp_files={
            ".mcp.json": {
                "mcpServers": {
                    "grafana": {"type": "http", "url": "${GRAFANA_MCP_URL:-http://svc:8000/mcp}"}
                }
            }
        },
    )
    assert extract_declared(str(root))[0]["cred_vars"] == ["GRAFANA_MCP_URL"]


def test_connectors_yaml_unhosted_url_strips_a_shell_style_default(tmp_path: Path) -> None:
    # Same stripping rule as `test_remote_url_var_strips_a_shell_style_default`,
    # but mined from a connectors.yaml connector's `unhosted_url` rather than an
    # `.mcp.json` `url`: the operator forwards LOCAL_MCP_PORT, not the literal
    # default, so `cred_vars` must name the bare var and neither the suffix nor
    # the fallback value.
    bundle = _connector_bundle(
        tmp_path / "unhosted_default",
        "connectors:\n"
        "  grafana:\n"
        "    image: grafana/mcp-grafana:0.17.2\n"
        "    unhosted_url: ${LOCAL_MCP_PORT:-8765}/mcp\n",
    )
    cred_vars = _row(extract_declared(str(bundle)), "grafana")["cred_vars"]
    assert cred_vars == ["LOCAL_MCP_PORT"], cred_vars


def test_remote_advisory_says_the_red_is_expected_and_names_the_secret() -> None:
    from curie_runner.check import _remote_advisory

    hint = _remote_advisory("grafana", ["GRAFANA_MCP_URL"])
    # The point of the advisory: distinguish "offline contract did its job" from
    # "your bundle is broken", which both rendered as red before.
    assert "cannot be reached offline" in hint
    assert "NOT evidence" in hint
    assert "--secret GRAFANA_MCP_URL" in hint


def test_remote_advisory_takes_precedence_over_the_authed_one() -> None:
    # A remote server WITH headers is both remote and authed. Unreachable-by-
    # design is the more specific explanation, so it should be the one shown.
    from curie_runner.check import evaluate

    declared = [
        {
            "name": "grafana",
            "source": ".mcp.json",
            "form": "bare_file",
            "authed": True,
            "cred_vars": ["TOKEN"],
            "remote": True,
        }
    ]
    result = evaluate(declared, [])
    hints = " ".join(result["hints"])
    assert "cannot be reached offline" in hints
    assert "was not exercised offline" not in hints


# --------------------------------------------------------------------------- #
# Declared connectors (#2348)
#
# `connectors.yaml` is the OTHER way a bundle declares an MCP server, and the
# check never read it: a bundle whose only server is a declared connector
# reported `declared: 0` and `verdict: green` -- byte-identical to a bundle that
# declares nothing at all. `curie skill check` is the documented diagnostic for
# "my tools are missing", so that false green is the defect, not a gap.
# --------------------------------------------------------------------------- #
_CONNECTORS_SOURCE = "connectors.yaml"

# The reason/hint substring the tier situation must be named by. A hosted
# connector on a tier that hosts nothing is "declared but not exercisable here"
# (#1093) -- a bare "never registered" reads as a bundle defect and sends the
# author debugging a bundle that is fine.
_TIER_REASON = "not exercisable"
_CURIE_COMMAND_RE = re.compile(r"curie [a-z]")


def _write_connectors(root: Path, body: str) -> Path:
    """Drop a `connectors.yaml` into an existing bundle root."""

    (root / "connectors.yaml").write_text(body, encoding="utf-8")
    return root


def _connector_bundle(root: Path, body: str, *, mcp: dict | None = None) -> Path:
    """A minimal loadable bundle whose declaration is a `connectors.yaml`."""

    _write_bundle(
        root,
        {"name": "b", "version": "0.1.0", "description": "t"},
        mcp_files={".mcp.json": mcp} if mcp is not None else None,
    )
    return _write_connectors(root, body)


def _row(rows: list[dict], name: str) -> dict:
    matching = [r for r in rows if r["name"] == name]
    assert len(matching) == 1, f"{name} must appear exactly once, got {rows}"
    return matching[0]


_HOSTED_YAML = (
    "connectors:\n"
    "  grafana:\n"
    "    image: grafana/mcp-grafana:0.17.2\n"
    "    args: [-t, streamable-http]\n"
    "    secrets: [GRAFANA_SERVICE_ACCOUNT_TOKEN]\n"
)
_HOSTED_UNHOSTED_YAML = (
    "connectors:\n"
    "  grafana:\n"
    "    image: grafana/mcp-grafana:0.17.2\n"
    "    unhosted_url: http://localhost:8765/mcp\n"
)
_BARE_HOSTED_YAML = "connectors:\n  plainbox:\n    image: x:1\n    args: [--serve]\n"


def test_extract_no_mcp_and_no_connectors_yaml_is_still_empty_green(tmp_path: Path) -> None:
    # Negative control for the whole feature: a bundle that genuinely declares
    # nothing must keep reporting nothing and stay green. If this ever goes red,
    # the connector rows are being invented rather than read.
    bundle = _write_bundle(tmp_path / "nothing", {"name": "no-decl", "version": "0.1.0"})
    assert extract_declared(str(bundle)) == []
    assert evaluate([], [])["verdict"] == "green"


def test_extract_hosted_connector_is_declared_and_authed(tmp_path: Path) -> None:
    # The #2348 bug in one assertion: this bundle declared a connector and the
    # check reported zero declared rows.
    bundle = _connector_bundle(tmp_path / "hosted", _HOSTED_YAML)
    rows = extract_declared(str(bundle))
    assert len(rows) == 1, rows
    row = _row(rows, "grafana")
    assert row["source"] == _CONNECTORS_SOURCE
    # `image` with no `unhosted_url`: Curie hosts it, and nothing else can.
    assert row["form"] == "hosted"
    # `secrets:` is a credential the credential-free offline check never
    # forwards, exactly like an `env` map on an `.mcp.json` server.
    assert row["authed"] is True
    assert row["cred_vars"] == ["GRAFANA_SERVICE_ACCOUNT_TOKEN"]


def test_hosted_connector_that_never_registered_is_not_green(tmp_path: Path) -> None:
    # The verdict half of the bug. Deletion check: drop the connector rows from
    # extract_declared and this false-greens, which is the shipped behaviour.
    bundle = _connector_bundle(tmp_path / "hosted_red", _HOSTED_YAML)
    result = evaluate(extract_declared(str(bundle)), [])
    assert result["verdict"] != "green", result
    assert result["reasons"], result
    # A bare "never registered" is wrong here: on a tier that hosts nothing the
    # connector is correctly absent, and the reason must say so rather than
    # accuse the bundle.
    reason = " ".join(result["reasons"])
    assert "grafana" in reason, reason
    assert _TIER_REASON in reason, reason
    # And the user is pointed at a real next command, naming the connector.
    hint = next((h for h in result["hints"] if "grafana" in h), "")
    assert hint, result["hints"]
    assert _CURIE_COMMAND_RE.search(hint), hint


def test_bare_hosted_connector_with_no_credential_is_not_authed(tmp_path: Path) -> None:
    # `authed` is a credential signal, not a "is a connector" signal: an
    # image+args connector carrying no secrets of any form must stay False, or
    # the authed advisory fires on every connector and stops meaning anything.
    bundle = _connector_bundle(tmp_path / "bare_hosted", _BARE_HOSTED_YAML)
    row = _row(extract_declared(str(bundle)), "plainbox")
    assert row["authed"] is False
    assert row["cred_vars"] == []


def test_every_credential_holder_is_authed_and_named(tmp_path: Path) -> None:
    # The credential holders a VALID `connectors.yaml` can carry: a plain
    # `secrets:` name, a SecretRef naming the env var in `name:`, a
    # `secret_files:` map keyed by the env var, and a remote connector's
    # `headers` carrying a ${VAR}. All of them mean the same thing here --
    # something the credential-free offline check never forwarded.
    bundle = _connector_bundle(
        tmp_path / "authed_forms",
        "connectors:\n"
        "  filed:\n"
        "    image: x:1\n"
        "    secret_files:\n"
        "      FILED_KUBECONFIG: /secrets/kubeconfig\n"
        "  plain:\n"
        "    image: x:1\n"
        "    secrets: [PLAIN_TOKEN]\n"
        "  reffed:\n"
        "    image: x:1\n"
        "    secrets:\n"
        "      - name: REFFED_TOKEN\n"
        "        from_secret: grafana-mcp\n"
        "  headed:\n"
        "    url: https://mcp.internal/mcp\n"
        "    headers:\n"
        '      Authorization: "Bearer ${HEADED_TOKEN}"\n',
    )
    rows = extract_declared(str(bundle))
    assert all(_row(rows, n)["authed"] is True for n in ("filed", "plain", "reffed", "headed"))
    # cred_vars names what `--secret` would have to forward. A SecretRef's env
    # var is its `name:`, never the Kubernetes Secret it points at.
    assert _row(rows, "plain")["cred_vars"] == ["PLAIN_TOKEN"]
    assert _row(rows, "reffed")["cred_vars"] == ["REFFED_TOKEN"]
    assert "grafana-mcp" not in _row(rows, "reffed")["cred_vars"]
    assert _row(rows, "headed")["cred_vars"] == ["HEADED_TOKEN"]
    # `secret_files` is KEYED BY the env var name, so the key belongs in
    # cred_vars and the VALUE -- a path into the runner filesystem -- never
    # does; without the key the advisory named no `--secret` at all while still
    # calling the connector authed.
    filed = _row(rows, "filed")["cred_vars"]
    assert filed == ["FILED_KUBECONFIG"], filed
    assert not any("/secrets" in var for var in filed), filed


def test_a_whole_file_refusal_contributes_no_rows_and_does_not_raise(tmp_path: Path) -> None:
    # `validate_connectors` refuses the WHOLE file when any connector in it is
    # invalid -- `sealed_secrets` is unsupported, so this file is refused
    # entirely. The census must refuse it too: `derive_mcp_servers` mounts
    # nothing and `approval_policy.connector_server_names` returns None for the
    # same input, and a census that reported rows where those two report none
    # would be the one reader of the file that disagrees with the mount.
    # Fail-soft, not fail-hard: zero rows, no exception (`validate_bundle`
    # reports the refusal itself, as `invalid_bundle`, on the real path).
    bundle = _connector_bundle(
        tmp_path / "refused_file",
        "connectors:\n"
        "  sealed:\n"
        "    image: x:1\n"
        "    sealed_secrets:\n"
        "      SEALED_TOKEN: AgBv3n2K\n"
        "  fine:\n"
        "    image: x:1\n"
        "    secrets: [FINE_TOKEN]\n",
    )
    rows = extract_declared(str(bundle))
    assert [r for r in rows if r["source"] == _CONNECTORS_SOURCE] == [], rows


def test_unhosted_url_connector_is_hosted_unhosted_and_can_still_go_green(
    tmp_path: Path,
) -> None:
    # The reachable-fallback case, and the one that must remain ABLE to pass:
    # a hosted connector carrying `unhosted_url` really is mountable on a tier
    # that hosts nothing, so when it registers with a tool the verdict is green.
    # Deletion check: make every connector row unconditionally red and this
    # fails -- the fix must not turn "declared" into "always red".
    bundle = _connector_bundle(tmp_path / "unhosted", _HOSTED_UNHOSTED_YAML)
    row = _row(extract_declared(str(bundle)), "grafana")
    assert row["form"] == "hosted_unhosted"
    result = evaluate(
        [row],
        [{"name": "grafana", "scope": "dynamic", "status": "connected", "tools": ["t"]}],
    )
    assert result["verdict"] == "green", result
    assert result["reasons"] == []


def test_unhosted_advisory_says_the_red_is_expected_not_a_broken_bundle() -> None:
    # `unhosted_url` IS an address this tier knows how to MOUNT, but it is not
    # one this tier can REACH: the check runs `--network none` by construction,
    # so `http://localhost:8765/mcp` is exactly as unreachable here as any
    # remote `url:`. The advisory must mirror `_remote_advisory`'s honesty --
    # claiming the result "is real" tells the author the red is about their
    # connector when it is really about the check's network policy, which is the
    # misdiagnosis #2348 exists to prevent.
    from curie_runner.check import _connector_advisory

    hint = _connector_advisory("grafana", "hosted_unhosted", ["GRAFANA_TOKEN"])
    assert "unhosted_url" in hint, hint
    assert "no network" in hint, hint
    assert "NOT evidence" in hint, hint
    assert "curie skill up" in hint, hint
    assert "--secret GRAFANA_TOKEN" in hint, hint

    # The plain hosted branch keeps its distinct meaning: nothing was mounted
    # because this tier hosts no connector at all.
    hosted = _connector_advisory("grafana", "hosted", [])
    assert "hosts no connector" in hosted, hosted
    assert _TIER_REASON in hosted, hosted


def test_a_forced_red_keeps_the_per_server_reasons_after_its_own(tmp_path: Path) -> None:
    # A timeout or a client-startup failure forces red with its own proximate
    # cause. That cause goes FIRST, but it must not REPLACE the per-declared
    # reasons `evaluate` computed: "declared connector X was not exercisable in
    # this tier" is the sentence #2348 is about, and substituting the list left
    # the author staring at a generic timeout with no mention of the connector.
    from curie_runner.check import _red_result

    bundle = _connector_bundle(tmp_path / "forced_red", _HOSTED_YAML)
    declared = extract_declared(str(bundle))
    result = _red_result(str(bundle), declared, "MCP init did not complete within 30s")
    assert result["verdict"] == "red", result
    assert result["reasons"][0] == "MCP init did not complete within 30s", result["reasons"]
    rest = " ".join(result["reasons"][1:])
    assert "grafana" in rest, result["reasons"]
    assert _TIER_REASON in rest, result["reasons"]


def test_remote_and_hosted_in_one_connectors_yaml_get_distinct_forms(tmp_path: Path) -> None:
    # `url:` is the remote form: already running, reachable from anywhere, and
    # nothing for Curie to host. Collapsing it into "hosted" would attach the
    # tier advisory to a connector whose absence really IS a defect.
    bundle = _connector_bundle(
        tmp_path / "mixed",
        "connectors:\n"
        "  internal:\n"
        "    url: https://mcp.internal/mcp\n"
        "  grafana:\n"
        "    image: grafana/mcp-grafana:0.17.2\n",
    )
    rows = extract_declared(str(bundle))
    assert len(rows) == 2, rows
    assert _row(rows, "internal")["form"] == "remote"
    assert _row(rows, "grafana")["form"] == "hosted"
    assert {r["source"] for r in rows} == {_CONNECTORS_SOURCE}

    # And the two forms must stay distinguishable in the ADVISORY, not only in
    # the row: the remote row gets the unreachable-by-design explanation
    # (#1093), the hosted row the tier one. Collapsing either way tells the
    # author to run the wrong command.
    hints = evaluate(rows, [])["hints"]
    internal = next(h for h in hints if "internal" in h)
    grafana = next(h for h in hints if "grafana" in h)
    assert "cannot be reached offline" in internal, internal
    assert _TIER_REASON not in internal, internal
    assert _TIER_REASON in grafana, grafana
    assert "cannot be reached offline" not in grafana, grafana


def test_empty_image_plus_url_stays_remote(tmp_path: Path) -> None:
    """`image: ""` alongside `url:` is a LEGAL validated spec, and it is remote.

    `validate_connectors` decides which form is present by TRUTHINESS
    (`bool(spec.image)` / `bool(spec.build)` / `bool(spec.url)`), while
    `ConnectorSpec.is_hosted` is `image is not None or build is not None`. Those
    are different tests, so this file validates with ZERO errors and reaches
    `extract_declared` with `image == ""`, a real `url`, AND `is_hosted` True.
    `_connector_form` must therefore test `url` FIRST: branching on `is_hosted`
    first calls this "hosted", flips `remote` to False, and swaps the remote
    advisory ("a red here is expected") for the tier one -- which is actively
    wrong, because an unreachable remote connector IS a potential real defect.
    Do not "simplify" the ordering.
    """

    bundle = _connector_bundle(
        tmp_path / "empty_image_remote",
        'connectors:\n  internal:\n    image: ""\n    url: https://mcp.internal/mcp\n',
    )
    row = _row(extract_declared(str(bundle)), "internal")
    assert row["form"] == "remote", row
    assert row["remote"] is True, row

    hint = next(h for h in evaluate([row], [])["hints"] if "internal" in h)
    assert "cannot be reached offline" in hint, hint
    assert _TIER_REASON not in hint, hint


def test_the_same_name_in_both_channels_is_counted_exactly_once(tmp_path: Path) -> None:
    # AC1's "each name counted exactly once". Tested against `extract_declared`
    # directly and not `run_check` on purpose: the real entrypoint REJECTS this
    # bundle outright (`connectors.duplicate_server`, covered separately), so
    # the census function is the only place the union's de-duplication is
    # observable at all. A union that appended blindly would report `declared:
    # 2` for one server and make the count unreconcilable.
    bundle = _connector_bundle(
        tmp_path / "same_name",
        "connectors:\n  grafana:\n    image: x:1\n",
        mcp={"mcpServers": {"grafana": {"type": "http", "url": "http://hand-written/mcp"}}},
    )
    rows = extract_declared(str(bundle))
    assert [r["name"] for r in rows].count("grafana") == 1, rows
    assert len(rows) == 1, rows


def test_build_form_connector_is_hosted(tmp_path: Path) -> None:
    # ADR 0113's `build:` form changes only where the image comes FROM; it is
    # the same hosted form and must classify identically, or a sourced connector
    # loses the tier advisory that a referenced one gets.
    bundle = _connector_bundle(
        tmp_path / "built",
        "connectors:\n"
        "  k8s-write:\n"
        "    build:\n"
        "      context: connectors/k8s-write\n"
        "      platforms: [linux/amd64]\n",
    )
    assert _row(extract_declared(str(bundle)), "k8s-write")["form"] == "hosted"


def test_build_form_connector_with_unhosted_url_is_hosted_unhosted(tmp_path: Path) -> None:
    # Same point as `test_build_form_connector_is_hosted`, but crossed with the
    # `unhosted_url` reachable-fallback case: ADR 0113's `build:` is the same
    # hosted form as `image:`, only sourced differently, so a `build:` connector
    # that also carries `unhosted_url` must classify "hosted_unhosted", not
    # collapse to plain "hosted" the way a bug keyed on `image` truthiness would.
    bundle = _connector_bundle(
        tmp_path / "built_unhosted",
        "connectors:\n"
        "  k8s-write:\n"
        "    build:\n"
        "      context: connectors/k8s-write\n"
        "      platforms: [linux/amd64]\n"
        "    unhosted_url: http://localhost:8765/mcp\n",
    )
    assert _row(extract_declared(str(bundle)), "k8s-write")["form"] == "hosted_unhosted"


def test_mcp_json_and_connectors_yaml_union_without_double_counting(tmp_path: Path) -> None:
    # Two channels, two rows, each attributed to the file it came from. The
    # union is what makes `declared: N` a count an operator can reconcile
    # against the bundle they wrote.
    bundle = _connector_bundle(
        tmp_path / "union",
        "connectors:\n  beta:\n    image: x:1\n",
        mcp={"mcpServers": {"alpha": {"command": "python3"}}},
    )
    rows = extract_declared(str(bundle))
    assert len(rows) == 2, rows
    assert _row(rows, "alpha")["source"] == ".mcp.json"
    assert _row(rows, "beta")["source"] == _CONNECTORS_SOURCE
    assert [r["name"] for r in rows].count("beta") == 1


def test_connector_registered_with_zero_tools_is_not_green(tmp_path: Path) -> None:
    # The capability probe failing is the case `skill check` exists for: the
    # server came up, so nothing is obviously broken, and it exposes no tools.
    bundle = _connector_bundle(tmp_path / "zero_tools", _HOSTED_UNHOSTED_YAML)
    result = evaluate(
        extract_declared(str(bundle)),
        [{"name": "grafana", "scope": "dynamic", "status": "connected", "tools": []}],
    )
    assert result["verdict"] != "green", result
    joined = " ".join(result["reasons"])
    assert "grafana" in joined, joined
    assert "zero tools" in joined, joined


@pytest.mark.parametrize(
    "body",
    [
        "connectors:\n  grafana:\n   image: x:1\n  \tbroken",  # not parseable YAML
        "connectors:\n  grafana:\n    image: x:1\n    nonsense_key: 1\n",  # does not validate
        "just a string",  # parses, wrong shape
    ],
)
def test_a_bad_connectors_yaml_contributes_no_rows_and_does_not_raise(
    tmp_path: Path, body: str
) -> None:
    # Same fail-soft trade `curie_runner.connectors._read` makes: the check is a
    # diagnostic, and crashing it removes the only tool the author has. Zero
    # rows, no exception. The bundle's own validation is what reports the file.
    bundle = _connector_bundle(tmp_path / f"bad_{abs(hash(body))}", body)
    rows = extract_declared(str(bundle))
    assert [r for r in rows if r["source"] == _CONNECTORS_SOURCE] == [], rows


def test_declared_row_contract_and_check_version_are_unchanged(tmp_path: Path) -> None:
    # The runner<->CLI JSON seam is frozen (plan Section 3). Adding connector
    # rows is additive: the version stays 1 and every row -- from either
    # channel -- keeps the keys the CLI already reads. A contract guard, not an
    # AC guard: CHECK_VERSION does not depend on the union, so nothing here
    # pins #2348's acceptance criteria.
    from curie_runner.check import CHECK_VERSION

    assert CHECK_VERSION == 1
    bundle = _connector_bundle(
        tmp_path / "contract",
        "connectors:\n  beta:\n    image: x:1\n",
        mcp={"mcpServers": {"alpha": {"command": "python3"}}},
    )
    for row in extract_declared(str(bundle)):
        assert {"name", "source", "form", "authed"} <= set(row), row


def _colliding_bundle(root: Path) -> Path:
    """A bundle declaring the SAME server name in both channels (#1118)."""

    (root / "skills" / "b").mkdir(parents=True, exist_ok=True)
    (root / "skills" / "b" / "SKILL.md").write_text(
        "---\nname: b\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    return _connector_bundle(
        root,
        "connectors:\n  grafana:\n    image: x:1\n",
        mcp={"mcpServers": {"grafana": {"type": "http", "url": "http://hand-written/mcp"}}},
    )


def test_a_name_in_both_channels_is_invalid_bundle_and_exits_2(tmp_path: Path) -> None:
    # One name, one owner (#1118). The check reads both channels now, so it
    # would be the natural place to "resolve" a collision -- it must not: the
    # bundle never loads, and that refusal is what keeps a silently-overridden
    # entry from reaching a runtime at all. Driven through `run_check` (not just
    # `validate_bundle`) because it is the CHECK's refusal that is the AC: the
    # union must not quietly de-duplicate a collision into one working row.
    bundle = _colliding_bundle(tmp_path / "collide")
    result = anyio.run(run_check, str(bundle))
    assert result["verdict"] == "invalid_bundle", result
    # No census at all for a bundle that never loads -- reporting a declared row
    # for a name whose ownership is ambiguous is the outcome being refused.
    assert result["declared"] == [], result
    assert result["reasons"], result
    # And the verdict carries the operator-visible exit code 2, not 1.
    from curie_runner.check import _EXIT_CODES

    assert _EXIT_CODES[result["verdict"]] == 2


def test_a_name_in_both_channels_exits_2_through_main(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The same refusal at the real entrypoint: `main()` maps the verdict to the
    # process exit code the CLI reads, and a collision must be 2 (invalid
    # bundle), never 1 (red) and never 0.
    from curie_runner.check import PLUGIN_DIR_ENV, main

    bundle = _colliding_bundle(tmp_path / "collide_main")
    monkeypatch.setenv(PLUGIN_DIR_ENV, str(bundle))
    monkeypatch.setenv("HOME", str(tmp_path))
    assert main() == 2


def test_run_check_on_the_issue_repro_declares_one_row_and_is_not_green(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The #2348 reproduction verbatim, at the real entrypoint: a bundle whose
    # ONLY declaration is a hosted `connectors.yaml` connector reported
    # `declared: 0, verdict: green` -- byte-identical to a bundle declaring
    # nothing. Asserting this at `run_check` (not at
    # `evaluate(extract_declared(...), [])`) is what pins AC3 on the path the
    # CLI actually runs.
    import curie_runner.check as check_mod

    bundle = _connector_bundle(
        tmp_path / "issue_repro",
        "connectors:\n"
        "  github:\n"
        "    image: ghcr.io/github/github-mcp-server:v0.6.0\n"
        "    args: [stdio]\n"
        "    secrets: [GITHUB_PERSONAL_ACCESS_TOKEN]\n",
    )
    monkeypatch.setattr(_FakeMcpClient, "servers", [])
    monkeypatch.setattr(_FakeMcpClient, "plugin_registered", [])
    monkeypatch.setattr(check_mod, "ClaudeSDKClient", _FakeMcpClient)
    result = anyio.run(run_check, str(bundle))
    assert len(result["declared"]) == 1, result["declared"]
    assert result["declared"][0]["name"] == "github", result["declared"]
    assert result["verdict"] != "green", result
    assert result["reasons"], result


def test_extract_declared_mounts_nothing_itself(tmp_path: Path) -> None:
    # extract_declared reports INTENT. Deriving a URL is a separate job with a
    # separate input (the release/agent/namespace scope, which the check does
    # not have), so a row must never carry a mount entry -- a URL invented here
    # would resolve nowhere and turn a clear "not exercisable" into a mid-turn
    # connection refused.
    bundle = _connector_bundle(tmp_path / "nomount", _HOSTED_YAML)
    for row in extract_declared(str(bundle)):
        assert not (set(row) & {"url", "type", "mcp_entry", "entry", "command"}), row


# --------------------------------------------------------------------------- #
# The derived entries reach the real client (#2348)
#
# Reading connectors.yaml only fixes the REPORT. The check also has to actually
# try the connector, or a `hosted_unhosted` bundle stays red for the same reason
# it was invisible: nothing ever mounted it.
# --------------------------------------------------------------------------- #
def _bare_name(registered_name: str) -> str:
    """The server name without any `plugin:<bundle>:` scoping prefix."""

    return registered_name.rsplit(":", 1)[-1]


class _FakeMcpClient:
    """Stands in for ClaudeSDKClient: connects, reports, disconnects. No turn.

    It HONORS the `mcp_servers` it was constructed with: a server from
    `servers` is reported only when that name was actually MOUNTED into the
    options. `plugin_registered` is the separate set the plugin itself brings
    up, which no mount is needed for.

    That distinction is the point. A fake that answered for a connector nobody
    mounted would leave every mount test green with `derive_mcp_servers`
    deleted, which is exactly the false confidence the #2348 review found.
    """

    servers: list[dict] = []
    plugin_registered: list[dict] = []

    def __init__(self, options: object) -> None:
        self.options = options
        self.mounted = set(getattr(options, "mcp_servers", None) or {})

    async def connect(self) -> None:
        return None

    async def get_mcp_status(self) -> dict:
        reported = [
            dict(s)
            for s in type(self).servers
            if _bare_name(str(s.get("name", ""))) in self.mounted
        ]
        reported.extend(dict(s) for s in type(self).plugin_registered)
        return {"mcpServers": reported}

    async def disconnect(self) -> None:
        return None


def _captured_mcp_servers(
    monkeypatch: pytest.MonkeyPatch, bundle: Path, *, servers: list[dict] | None = None
) -> dict:
    """Run `run_check` against a fake client and return the mcp_servers kwarg."""

    import curie_runner.check as check_mod

    captured: dict = {}
    real_build_options = check_mod.build_options

    def _spy(**kwargs: object) -> object:
        captured.update(kwargs)
        return real_build_options(**kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(check_mod, "build_options", _spy)
    monkeypatch.setattr(_FakeMcpClient, "servers", list(servers or []))
    monkeypatch.setattr(_FakeMcpClient, "plugin_registered", [])
    monkeypatch.setattr(check_mod, "ClaudeSDKClient", _FakeMcpClient)
    anyio.run(run_check, str(bundle))
    assert "mcp_servers" in captured, captured
    return dict(captured["mcp_servers"] or {})


def test_unhosted_connector_is_mounted_for_the_probe(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # An `unhosted_url` connector is reachable on this tier, so the check must
    # mount it and genuinely find out -- that is the difference between a
    # diagnostic and a restatement of the bundle file.
    # And it must mount THAT address: the entry has to be the one
    # `unhosted_mcp_entry` derives from the spec, not a cluster-scoped
    # `mcp_entry` built from an invented release/agent/namespace, which would
    # resolve nowhere and turn a clear diagnostic into a connection refused.
    # Compared against the renderer rather than a hardcoded shape so this stays
    # true if the entry's shape evolves.
    from plugin_format.connector_render import unhosted_mcp_entry
    from plugin_format.connectors import validate_connectors
    from plugin_format.yaml_loader import safe_load_unique

    bundle = _connector_bundle(tmp_path / "mount_unhosted", _HOSTED_UNHOSTED_YAML)
    mounted = _captured_mcp_servers(monkeypatch, bundle)
    assert "grafana" in mounted, mounted

    parsed, errors = validate_connectors(
        safe_load_unique((bundle / "connectors.yaml").read_text(encoding="utf-8"))
    )
    assert parsed is not None, errors
    assert mounted["grafana"] == unhosted_mcp_entry(parsed.connectors["grafana"]), mounted


def test_hosted_only_connector_mounts_nothing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # No scope, no `unhosted_url`: there is no address, and inventing one turns
    # "declared but not exercisable" into a connection refused. Mount nothing
    # and let the tier reason explain the red.
    bundle = _connector_bundle(tmp_path / "mount_hosted", _HOSTED_YAML)
    assert _captured_mcp_servers(monkeypatch, bundle) == {}


def test_a_mounted_connector_that_answers_makes_the_run_green(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # End to end over the fake client: declared from connectors.yaml, mounted,
    # registered with a tool -> green. The fake only answers for servers that
    # were actually mounted, so deleting the `derive_mcp_servers` mount from
    # `_connect_and_poll` flips this red -- without that, the same bundle can
    # never be anything but red, which is the failure this pair guards.
    import curie_runner.check as check_mod

    bundle = _connector_bundle(tmp_path / "green_e2e", _HOSTED_UNHOSTED_YAML)
    monkeypatch.setattr(
        _FakeMcpClient,
        "servers",
        [{"name": "grafana", "scope": "dynamic", "status": "connected", "tools": [_tool("t")]}],
    )
    monkeypatch.setattr(_FakeMcpClient, "plugin_registered", [])
    monkeypatch.setattr(check_mod, "ClaudeSDKClient", _FakeMcpClient)
    result = anyio.run(run_check, str(bundle))
    assert [r["name"] for r in result["declared"]] == ["grafana"], result
    assert result["verdict"] == "green", result


def test_an_unmounted_connector_cannot_answer_and_stays_red(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    # The negative half of the pair, and what makes the green above mean
    # something: the SAME connected-with-tools server, on a bundle whose
    # connector has no `unhosted_url` to mount, is never reported -- so a
    # hosted-only bundle cannot borrow another tier's green.
    import curie_runner.check as check_mod

    bundle = _connector_bundle(tmp_path / "unmounted", _HOSTED_YAML)
    monkeypatch.setattr(
        _FakeMcpClient,
        "servers",
        [{"name": "grafana", "scope": "dynamic", "status": "connected", "tools": [_tool("t")]}],
    )
    monkeypatch.setattr(_FakeMcpClient, "plugin_registered", [])
    monkeypatch.setattr(check_mod, "ClaudeSDKClient", _FakeMcpClient)
    result = anyio.run(run_check, str(bundle))
    assert [r["name"] for r in result["declared"]] == ["grafana"], result
    assert result["verdict"] != "green", result
