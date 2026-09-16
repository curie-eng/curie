import json
import shutil
from pathlib import Path

import pytest
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    ValidationResult,
    validate_bundle,
    validate_pattern,
)

FIXTURES = Path(__file__).parent / "fixtures"

# The bundle whose manifest carries the string-pointer mcpServers form the real
# Claude Code loader silently ignores (#336). Owned by runner/, read-only here:
# it exists to be rejected, and #540 makes plugin_format a second, static gate on
# it so `curie skill check` (which boots a container to observe it) is no longer
# the only one.
_RED_POINTER = Path(__file__).parents[3] / "runner" / "tests" / "fixtures" / "mcp_red_pointer"

_POINTER_CODE = "mcp.declared_pointer"
_CONFUSABLE_CODE = "skill.tools_confusable"


def _codes(path: Path) -> set[str]:
    return {issue.code for issue in validate_bundle(path).errors}


def _bundle(tmp_path: Path, manifest: str) -> Path:
    """Write a minimal bundle carrying the given manifest JSON."""
    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin" / "plugin.json").write_text(manifest, encoding="utf-8")
    return tmp_path


def test_valid_bundle_passes() -> None:
    result = validate_bundle(FIXTURES / "valid_bundle")
    assert result.valid, result.errors
    assert result.errors == []


def test_missing_manifest_is_reported(tmp_path: Path) -> None:
    result = validate_bundle(tmp_path)
    assert not result.valid
    assert "manifest.missing" in {i.code for i in result.errors}


def test_non_directory_path_is_reported(tmp_path: Path) -> None:
    stray = tmp_path / "not-a-dir"
    stray.write_text("x", encoding="utf-8")
    result = validate_bundle(stray)
    assert not result.valid
    assert {i.code for i in result.errors} == {"bundle.missing"}


def test_non_kebab_name_is_reported() -> None:
    assert "manifest.name_invalid" in _codes(FIXTURES / "bad_manifest_name")


def test_skill_missing_description_is_reported() -> None:
    codes = _codes(FIXTURES / "bad_skill")
    assert "skill.frontmatter_invalid" in codes


def test_mcp_server_without_command_or_url_is_reported() -> None:
    assert "mcp.server_incomplete" in _codes(FIXTURES / "bad_mcp")


def test_inline_object_mcp_declaration_stays_valid(tmp_path: Path) -> None:
    # The fix the string-pointer error names. It must keep validating clean.
    bundle = _bundle(tmp_path, '{"name": "demo", "mcpServers": {"crm": {"command": "crm-server"}}}')
    result = validate_bundle(bundle)
    assert result.valid, result.errors


def test_inline_manifest_mcp_server_is_validated() -> None:
    # The manifest mcpServers field (inline object) is a supported declaration
    # and must be validated, not just a root .mcp.json file.
    assert "mcp.server_incomplete" in _codes(FIXTURES / "bad_mcp_inline")


def test_error_messages_carry_location_and_are_actionable() -> None:
    result = validate_bundle(FIXTURES / "bad_skill")
    issue = next(i for i in result.errors if i.code == "skill.frontmatter_invalid")
    assert issue.location.endswith("SKILL.md")
    assert "description" in issue.message


def _bundle(tmp_path: Path, manifest: str) -> Path:
    """Write a minimal valid bundle carrying the given manifest JSON."""
    (tmp_path / ".claude-plugin").mkdir()
    (tmp_path / ".claude-plugin" / "plugin.json").write_text(manifest, encoding="utf-8")
    return tmp_path


def test_inline_valid_pretooluse_hook_passes(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "hooks": {"PreToolUse": [{"matcher": "Bash", '
        '"hooks": [{"type": "command", "command": "./guard.sh"}]}]}}',
    )
    result = validate_bundle(bundle)
    assert result.valid, result.errors


def test_command_hook_without_command_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "hooks": {"PreToolUse": [{"matcher": "Bash", '
        '"hooks": [{"type": "command"}]}]}}',
    )
    assert "hooks.command_missing" in _codes(bundle)


def test_malformed_hooks_shape_is_rejected(tmp_path: Path) -> None:
    # A matcher entry must be an object with a hooks list, not a bare string.
    bundle = _bundle(tmp_path, '{"name": "demo", "hooks": {"PreToolUse": ["nope"]}}')
    assert "hooks.invalid" in _codes(bundle)


def test_declared_hooks_file_missing_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "demo", "hooks": "hooks/hooks.json"}')
    assert "hooks.declared_missing" in _codes(bundle)


def test_declared_hooks_file_is_validated(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "demo", "hooks": "hooks/hooks.json"}')
    hooks_dir = bundle / "hooks"
    hooks_dir.mkdir()
    (hooks_dir / "hooks.json").write_text(
        '{"PreToolUse": [{"hooks": [{"type": "command"}]}]}', encoding="utf-8"
    )
    assert "hooks.command_missing" in _codes(bundle)


def test_valid_cron_and_webhook_triggers_pass(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "triggers": ['
        '{"type": "cron", "schedule": "0 9 * * 1-5"}, '
        '{"type": "webhook", "path": "/hooks/deploy"}]}',
    )
    assert validate_bundle(bundle).valid


def test_cron_trigger_without_schedule_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "demo", "triggers": [{"type": "cron"}]}')
    assert "triggers.cron_missing_schedule" in _codes(bundle)


def test_webhook_trigger_without_path_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "demo", "triggers": [{"type": "webhook"}]}')
    assert "triggers.webhook_missing_path" in _codes(bundle)


def test_unknown_trigger_type_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "demo", "triggers": [{"type": "kafka"}]}')
    assert "triggers.unknown_type" in _codes(bundle)


def test_malformed_triggers_shape_is_rejected(tmp_path: Path) -> None:
    # A non-list triggers value is rejected (the manifest type gate catches it).
    bundle = _bundle(tmp_path, '{"name": "demo", "triggers": "nope"}')
    assert not validate_bundle(bundle).valid


def test_trigger_entry_not_object_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "demo", "triggers": ["nope"]}')
    assert "triggers.invalid" in _codes(bundle)


def test_valid_secrets_policy_passes(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path, '{"name": "demo", "secrets": ["GITHUB_PERSONAL_ACCESS_TOKEN", "API_KEY"]}'
    )
    assert validate_bundle(bundle).valid


def test_non_env_var_secret_name_is_rejected(tmp_path: Path) -> None:
    # A lowercase/hyphenated name cannot be forwarded as an env var -> rejected.
    bundle = _bundle(tmp_path, '{"name": "demo", "secrets": ["github-token"]}')
    assert "secrets.name_invalid" in _codes(bundle)


def test_reserved_curie_secret_name_is_rejected(tmp_path: Path) -> None:
    # CURIE_* names are reserved platform boot-env keys.
    bundle = _bundle(tmp_path, '{"name": "demo", "secrets": ["CURIE_BUDGET"]}')
    assert "secrets.name_reserved" in _codes(bundle)


# The four runner-owned credential keys are NOT CURIE_-prefixed, so the #445
# prefix fence never saw them: a bundle could declare `ANTHROPIC_BASE_URL` and
# silently redirect the model. #457 rejects them at the same deploy gate.
_RESERVED_CREDENTIAL_KEYS = [
    "ANTHROPIC_BASE_URL",
    "ANTHROPIC_API_KEY",
    "CLAUDE_CODE_OAUTH_TOKEN",
    "ANTHROPIC_AUTH_TOKEN",
]


@pytest.mark.parametrize("name", _RESERVED_CREDENTIAL_KEYS)
def test_reserved_credential_secret_name_is_rejected(tmp_path: Path, name: str) -> None:
    bundle = _bundle(tmp_path, f'{{"name": "demo", "secrets": ["{name}"]}}')
    assert "secrets.name_reserved" in _codes(bundle)


# #487: generic redirect/capture-capable env is reserved too -- a proxy, an extra
# trusted CA, or custom headers on the model call reach the same capture end state.
_REDIRECT_CAPTURE_KEYS = [
    "HTTPS_PROXY",
    "HTTP_PROXY",
    "NODE_EXTRA_CA_CERTS",
    "ANTHROPIC_CUSTOM_HEADERS",
]


@pytest.mark.parametrize("name", _REDIRECT_CAPTURE_KEYS)
def test_reserved_redirect_capture_secret_name_is_rejected(tmp_path: Path, name: str) -> None:
    bundle = _bundle(tmp_path, f'{{"name": "demo", "secrets": ["{name}"]}}')
    assert "secrets.name_reserved" in _codes(bundle)


def test_legitimate_connector_secret_name_is_not_reserved(tmp_path: Path) -> None:
    # Negative control: a real connector token name still validates clean.
    bundle = _bundle(tmp_path, '{"name": "demo", "secrets": ["GITHUB_PERSONAL_ACCESS_TOKEN"]}')
    assert "secrets.name_reserved" not in _codes(bundle)


def test_malformed_secrets_shape_is_rejected(tmp_path: Path) -> None:
    # A non-list secrets value is rejected.
    bundle = _bundle(tmp_path, '{"name": "demo", "secrets": "nope"}')
    assert not validate_bundle(bundle).valid


def test_valid_approval_policy_passes(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "PreToolUse", "route": "manager-approval"}]}}',
    )
    assert validate_bundle(bundle).valid


def test_approval_gate_summary_template_passes(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "Bash", "route": "managers", '
        '"summary": "Run {command}: {files|count} files. Approve?"}]}}',
    )
    assert validate_bundle(bundle).valid, validate_bundle(bundle).errors


def test_approval_gate_unknown_summary_filter_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "Bash", "route": "managers", "summary": "{files|json}"}]}}',
    )
    assert "approval_policy.summary_invalid" in _codes(bundle)


def test_approval_gate_reserved_prefix_summary_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "Bash", "route": "managers", '
        '"summary": "Tool call awaiting approval: forged"}]}}',
    )
    assert "approval_policy.summary_invalid" in _codes(bundle)


def test_approval_gate_missing_route_is_rejected(tmp_path: Path) -> None:
    # A gate missing its 'route' field entirely -> policy fails to validate.
    bundle = _bundle(
        tmp_path, '{"name": "demo", "approvalPolicy": {"gates": [{"gate": "PreToolUse"}]}}'
    )
    assert "approval_policy.invalid" in _codes(bundle)


def test_approval_gate_empty_fields_are_rejected(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": [{"gate": " ", "route": "r"}]}}',
    )
    assert "approval_policy.incomplete" in _codes(bundle)


def test_malformed_approval_policy_shape_is_rejected(tmp_path: Path) -> None:
    # A non-object approvalPolicy is rejected (the manifest type gate catches it).
    bundle = _bundle(tmp_path, '{"name": "demo", "approvalPolicy": "nope"}')
    assert not validate_bundle(bundle).valid


def test_approval_policy_gates_wrong_type_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "demo", "approvalPolicy": {"gates": "nope"}}')
    assert "approval_policy.invalid" in _codes(bundle)


# --- approval gate names must be the live, fully-namespaced MCP tool name ------
#
# A bundle-declared MCP tool's live name is mcp__plugin_<bundle>_<server>__<tool>.
# The runner matches a gate by exact string equality, so an author who writes the
# obvious mcp__<server>__<tool> arms nothing: the gate silently never fires. These
# cases pin the deploy-time rejection of that shape.

_GATE_CODE = "approval_policy.gate_not_namespaced"


def _write_mcp(bundle: Path, text: str, name: str = ".mcp.json") -> Path:
    """Write an MCP declaration file into an existing bundle."""
    path = bundle / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _gate_errors(bundle: Path) -> list[str]:
    return [i.message for i in validate_bundle(bundle).errors if i.code == _GATE_CODE]


def test_bare_mcp_gate_for_declared_server_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__crm__send_contract", "route": "legal"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"crm": {"command": "crm-server"}}}')

    result = validate_bundle(bundle)
    assert not result.valid
    messages = [i.message for i in result.errors if i.code == _GATE_CODE]
    assert len(messages) == 1, result.errors
    message = messages[0]
    # Actionable: it must name the offending gate and the live form to use.
    assert "mcp__crm__send_contract" in message
    assert "mcp__plugin_demo_crm__" in message
    # And it must point at the escape hatch for a live name the bundle
    # does not declare, rather than dead-ending the author.
    assert "CURIE_APPROVAL_REQUIRED_TOOLS" in message


def test_builtin_tool_gate_passes(tmp_path: Path) -> None:
    # A gate with no mcp__ prefix names a built-in tool and is never touched,
    # even when the bundle also declares an MCP server.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "Bash", "route": "legal"}, '
        '{"gate": "PreToolUse", "route": "manager-approval"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"crm": {"command": "crm-server"}}}')

    result = validate_bundle(bundle)
    assert result.valid, result.errors
    assert not [i for i in result.errors if i.code.startswith("approval_policy.")]


# --- grantable-via-policy gates (#558): an ambiguous grantable route is rejected -
#
# A gate the operator marks ``grantableViaPolicy: true`` opts that gate's policy
# approval into minting a one-shot grant for the tool it names. When two grantable
# gates claim the SAME route with DIFFERENT tools, the route cannot resolve to a
# single grant tool, so it is rejected at deploy with
# ``approval_policy.grant_route_ambiguous``.

_GRANT_AMBIGUOUS_CODE = "approval_policy.grant_route_ambiguous"


def test_single_grantable_gate_passes(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "close_issue", "route": "deal-desk", "grantableViaPolicy": true}]}}',
    )
    result = validate_bundle(bundle)
    assert result.valid, result.errors
    assert _GRANT_AMBIGUOUS_CODE not in {i.code for i in result.errors}


def test_two_grantable_gates_same_route_different_tool_is_ambiguous(
    tmp_path: Path,
) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "close_issue", "route": "deal-desk", "grantableViaPolicy": true}, '
        '{"gate": "escalate", "route": "deal-desk", "grantableViaPolicy": true}]}}',
    )
    assert _GRANT_AMBIGUOUS_CODE in _codes(bundle)


def test_two_grantable_gates_same_route_same_tool_is_not_ambiguous(
    tmp_path: Path,
) -> None:
    # One route, one DISTINCT tool declared twice: a duplicate, not a conflict.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "close_issue", "route": "deal-desk", "grantableViaPolicy": true}, '
        '{"gate": "close_issue", "route": "deal-desk", "grantableViaPolicy": true}]}}',
    )
    assert _GRANT_AMBIGUOUS_CODE not in _codes(bundle)


def test_non_grantable_duplicate_route_pair_is_not_ambiguous(tmp_path: Path) -> None:
    # Two gates share a route with different tools, but neither opts in, so the
    # grant-ambiguity check ignores them entirely.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "close_issue", "route": "deal-desk"}, '
        '{"gate": "escalate", "route": "deal-desk"}]}}',
    )
    assert _GRANT_AMBIGUOUS_CODE not in _codes(bundle)


def test_correctly_namespaced_gate_passes_without_asserting_the_tool(tmp_path: Path) -> None:
    # send_contract is a tool nothing declares and nothing could know without
    # running the server. The prefix is correct, so the gate passes: the suffix
    # is never inspected.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__plugin_demo_crm__send_contract", "route": "legal"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"crm": {"command": "crm-server"}}}')

    result = validate_bundle(bundle)
    assert result.valid, result.errors


def test_gate_naming_an_undeclared_server_is_rejected(tmp_path: Path) -> None:
    # Correct prefix shape, but 'ghost' is not a server this bundle declares.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__plugin_demo_ghost__x", "route": "legal"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"crm": {"command": "crm-server"}}}')

    assert _GATE_CODE in _codes(bundle)


def test_gate_for_inline_manifest_mcp_servers_is_resolved(tmp_path: Path) -> None:
    # The manifest mcpServers field carries an inline dict rather than a path.
    good_dir = tmp_path / "good"
    good_dir.mkdir()
    good = _bundle(
        good_dir,
        '{"name": "demo", "mcpServers": {"crm": {"command": "crm-server"}}, '
        '"approvalPolicy": {"gates": ['
        '{"gate": "mcp__plugin_demo_crm__send_contract", "route": "legal"}]}}',
    )
    assert validate_bundle(good).valid, validate_bundle(good).errors

    bad_dir = tmp_path / "bad"
    bad_dir.mkdir()
    bad = _bundle(
        bad_dir,
        '{"name": "demo", "mcpServers": {"crm": {"command": "crm-server"}}, '
        '"approvalPolicy": {"gates": ['
        '{"gate": "mcp__crm__send_contract", "route": "legal"}]}}',
    )
    assert _GATE_CODE in _codes(bad)


def test_gate_resolved_across_both_inline_and_root_mcp_json(tmp_path: Path) -> None:
    # A bundle with an inline dict AND a distinct root .mcp.json declares BOTH
    # sets of servers; gates for either must pass.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "mcpServers": {"alpha": {"command": "alpha-server"}}, '
        '"approvalPolicy": {"gates": ['
        '{"gate": "mcp__plugin_demo_alpha__x", "route": "legal"}, '
        '{"gate": "mcp__plugin_demo_beta__y", "route": "legal"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"beta": {"command": "beta-server"}}}')

    result = validate_bundle(bundle)
    assert result.valid, result.errors


def test_string_pointer_mcp_declaration_does_not_add_a_gate_error(tmp_path: Path) -> None:
    # Inverted from test_gate_for_string_pointer_mcp_declaration_is_resolved: the
    # string-pointer form is now rejected outright, so the file it points at is
    # never read and the declared-server set is unknowable. The gate cross-check
    # must stay silent rather than telling the author their correctly-namespaced
    # gate names a server they did not declare -- the wrong fix.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "mcpServers": "config/servers.json", '
        '"approvalPolicy": {"gates": ['
        '{"gate": "mcp__plugin_demo_crm__send_contract", "route": "legal"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"crm": {"command": "crm-server"}}}', "config/servers.json")

    codes = _codes(bundle)
    assert _POINTER_CODE in codes
    assert _GATE_CODE not in codes


def test_mcp_gate_rejected_when_bundle_declares_no_servers(tmp_path: Path) -> None:
    # An approvalPolicy with an mcp__ gate but no MCP declaration anywhere.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__crm__send_contract", "route": "legal"}]}}',
    )

    messages = _gate_errors(bundle)
    assert len(messages) == 1, messages
    # The message must state the bundle declares none rather than print an
    # empty list at the author.
    assert "no MCP servers" in messages[0]


def test_invalid_json_mcp_declaration_does_not_add_a_gate_error(tmp_path: Path) -> None:
    # An unreadable declaration is not an empty one: the bundle already fails on
    # the MCP error, and stacking a misleading gate error on top would send the
    # author chasing the wrong fix.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__crm__send_contract", "route": "legal"}]}}',
    )
    _write_mcp(bundle, "{not json")

    codes = _codes(bundle)
    assert "mcp.invalid_json" in codes
    assert _GATE_CODE not in codes


def test_invalid_mcp_config_does_not_add_a_gate_error(tmp_path: Path) -> None:
    # Valid JSON that fails McpConfig validation. This is the layer where
    # conflating "could not read" with "read, and it was empty" is the tempting
    # shortcut: an empty set here would report every mcp__ gate as naming an
    # undeclared server, on top of the real mcp.invalid error.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__crm__send_contract", "route": "legal"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": []}')

    codes = _codes(bundle)
    assert "mcp.invalid" in codes
    assert _GATE_CODE not in codes


def test_missing_string_pointer_mcp_path_does_not_add_a_gate_error(tmp_path: Path) -> None:
    # Re-pointed from test_declared_missing_mcp_path_does_not_add_a_gate_error:
    # whether the pointed-at file exists no longer matters, because the form
    # itself is the error. Either way the declaration is unreadable and the gate
    # cross-check stays silent.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "mcpServers": "config/servers.json", '
        '"approvalPolicy": {"gates": ['
        '{"gate": "mcp__crm__send_contract", "route": "legal"}]}}',
    )

    codes = _codes(bundle)
    assert _POINTER_CODE in codes
    assert _GATE_CODE not in codes


def test_bundle_declaring_zero_mcp_servers_still_rejects_an_mcp_gate(tmp_path: Path) -> None:
    # The other side of the unreadable cases: a READABLE declaration that
    # declares no servers. Same empty prefix set, opposite verdict. This pair is
    # what makes empty and unreadable provably distinct facts.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__crm__send_contract", "route": "legal"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {}}')

    codes = _codes(bundle)
    assert "mcp.invalid" not in codes
    assert "mcp.invalid_json" not in codes
    assert _GATE_CODE in codes


def test_hyphenated_bundle_and_underscored_server_names_resolve(tmp_path: Path) -> None:
    # Live names are not mangled: a bundle name keeps its hyphens and a server
    # key keeps its underscores. This is why the rule constructs the expected
    # prefix from what the bundle declares instead of parsing the gate string.
    manifest = (
        '{{"name": "github-issues", '
        '"mcpServers": {{"local_tools": {{"command": "tools-server"}}}}, '
        '"approvalPolicy": {{"gates": [{{"gate": "{gate}", "route": "legal"}}]}}}}'
    )
    good_dir = tmp_path / "good"
    good_dir.mkdir()
    good = _bundle(good_dir, manifest.format(gate="mcp__plugin_github-issues_local_tools__x"))
    assert validate_bundle(good).valid, validate_bundle(good).errors

    bad_dir = tmp_path / "bad"
    bad_dir.mkdir()
    bad = _bundle(bad_dir, manifest.format(gate="mcp__local_tools__x"))
    bad_messages = _gate_errors(bad)
    assert len(bad_messages) == 1, bad_messages
    assert "mcp__plugin_github-issues_local_tools__" in bad_messages[0]


def test_malformed_mcp_gate_is_rejected(tmp_path: Path) -> None:
    # A gate that is bare 'mcp__' or otherwise matches no expected prefix falls
    # to the general rule; no special case exists for it.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": [{"gate": "mcp__", "route": "legal"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"crm": {"command": "crm-server"}}}')

    assert _GATE_CODE in _codes(bundle)


def test_gate_with_leading_whitespace_is_rejected(tmp_path: Path) -> None:
    # Leading whitespace hides the mcp__ prefix from a naive startswith check,
    # so the gate looks like a built-in tool and passes green. But the runner
    # strips the value before matching, leaving the bare mcp__crm__send_contract
    # that never equals the live mcp__plugin_demo_crm__send_contract -- the gate
    # arms nothing. The validator must inspect the stripped value, like runtime.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": " mcp__crm__send_contract", "route": "legal"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"crm": {"command": "crm-server"}}}')

    assert _GATE_CODE in _codes(bundle)


def test_mcp_gate_with_empty_tool_suffix_is_rejected(tmp_path: Path) -> None:
    # The prefix is correct but there is no tool name after it, so the gate can
    # never equal a real tool like mcp__plugin_demo_crm__send_contract. The
    # startswith check alone passes it green; the validator must require at least
    # one character after the matched prefix.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__plugin_demo_crm__", "route": "legal"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"crm": {"command": "crm-server"}}}')

    assert _GATE_CODE in _codes(bundle)


def test_each_offending_gate_is_reported_at_its_own_location(tmp_path: Path) -> None:
    # Gates are checked independently at gates[i]; no dedupe.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__crm__send_contract", "route": "legal"}, '
        '{"gate": "mcp__crm__send_contract", "route": "finance"}]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"crm": {"command": "crm-server"}}}')

    locations = [i.location for i in validate_bundle(bundle).errors if i.code == _GATE_CODE]
    # Both entries are reported, each anchored to its own gates[i].
    assert len(locations) == 2, locations
    assert any("gates[0]" in loc for loc in locations)
    assert any("gates[1]" in loc for loc in locations)


# --- the string-pointer mcpServers form is rejected at validate ---------------
#
# `"mcpServers": "config/mcp.json"` parses clean and the pointed-at file even
# validates, but the real loader ignores the form entirely: the servers never
# register. Validating the file it points at is validating something that never
# loads. The form itself is the error (#540, ref #336).


def test_string_pointer_mcp_declaration_is_rejected(tmp_path: Path) -> None:
    # File present AND itself valid -- the case that validates clean today.
    bundle = _bundle(tmp_path, '{"name": "demo", "mcpServers": "config/servers.json"}')
    _write_mcp(bundle, '{"mcpServers": {"crm": {"command": "crm-server"}}}', "config/servers.json")

    result = validate_bundle(bundle)
    assert not result.valid
    messages = [i.message for i in result.errors if i.code == _POINTER_CODE]
    assert len(messages) == 1, result.errors
    # Actionable: it must name the inline-object fix, not just say "no".
    assert "mcpServers" in messages[0]
    assert "inline" in messages[0]


def test_red_pointer_fixture_is_rejected_without_booting_a_container() -> None:
    # The #336 fixture, caught statically from one JSON field.
    result = validate_bundle(_RED_POINTER)
    assert not result.valid
    assert _POINTER_CODE in {i.code for i in result.errors}


# --- the tools / allowed-tools confusable ------------------------------------
#
# `extra="allow"` lets `tools:` parse clean while allowed_tools stays None, so
# the skill silently gets no tools. A targeted confusable check rejects it by
# name. A blanket extra="forbid" is forbidden (packages/CLAUDE.md:83-88): real
# Claude Code bundles carry keys this MVP does not model.

_CONFUSABLE_KEYS = ["tools", "allowed_tools", "allowedTools"]


def _write_skill(bundle: Path, frontmatter: str) -> Path:
    """Write a skills/demo/SKILL.md carrying the given frontmatter body."""
    skill = bundle / "skills" / "demo" / "SKILL.md"
    skill.parent.mkdir(parents=True, exist_ok=True)
    skill.write_text(
        f"---\nname: demo\ndescription: A demo skill.\n{frontmatter}---\n\n# Demo\n",
        encoding="utf-8",
    )
    return skill


@pytest.mark.parametrize("key", _CONFUSABLE_KEYS)
def test_confusable_tools_key_without_allowed_tools_is_rejected(tmp_path: Path, key: str) -> None:
    bundle = _bundle(tmp_path, '{"name": "demo"}')
    _write_skill(bundle, f"{key}:\n  - Bash\n")

    result = validate_bundle(bundle)
    assert not result.valid
    messages = [i.message for i in result.errors if i.code == _CONFUSABLE_CODE]
    assert len(messages) == 1, result.errors
    # It must name the offending key AND the correct one -- telling the author
    # the right key is the entire point.
    assert key in messages[0]
    assert "allowed-tools" in messages[0]


def test_correct_allowed_tools_key_validates_clean(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "demo"}')
    _write_skill(bundle, "allowed-tools:\n  - Bash\n")

    result = validate_bundle(bundle)
    assert result.valid, result.errors


def test_unknown_non_confusable_frontmatter_key_validates_clean(tmp_path: Path) -> None:
    # The leniency guardrail (packages/CLAUDE.md:83-88). This test is what a
    # blanket extra="forbid" would break, and it is why the check is targeted.
    bundle = _bundle(tmp_path, '{"name": "demo"}')
    _write_skill(bundle, "allowed-tools:\n  - Bash\nsome-future-claude-key: x\n")

    result = validate_bundle(bundle)
    assert result.valid, result.errors


def test_unknown_key_without_allowed_tools_validates_clean(tmp_path: Path) -> None:
    # Same leniency guardrail, but on the path where _check_tools_confusable
    # actually runs its loop: no 'allowed-tools' key, so the early return above
    # does not short-circuit before the unknown key is checked against the
    # confusable allowlist. Proves the check is a targeted three-key allowlist,
    # not a de-facto extra="forbid" over all unrecognized keys.
    bundle = _bundle(tmp_path, '{"name": "demo"}')
    _write_skill(bundle, "some-future-claude-key: x\n")

    result = validate_bundle(bundle)
    assert result.valid, result.errors


@pytest.mark.parametrize("key", _CONFUSABLE_KEYS)
def test_confusable_key_alongside_allowed_tools_validates_clean(tmp_path: Path, key: str) -> None:
    # An author who already has the right key is not confused, whatever else the
    # bundle carries. Erroring here would reject real Claude Code bundles.
    bundle = _bundle(tmp_path, '{"name": "demo"}')
    _write_skill(bundle, f"allowed-tools:\n  - Bash\n{key}:\n  - Read\n")

    result = validate_bundle(bundle)
    assert result.valid, result.errors


# --- a gate may name a connectors.yaml connector's tool (#1495) -----------------
#
# A bundle that declares its tool surface through connectors.yaml has no
# mcpServers and no .mcp.json, so before #1495 the accepted-name set was empty and
# EVERY gate it could write was rejected as not-namespaced: a connector tool could
# not be gated at all. The connector's live name is the bare
# mcp__<connector>__<tool> (it rides ClaudeAgentOptions.mcp_servers, not the plugin
# loader), so that -- and NOT the plugin form -- is what validates.


def _write_connectors(bundle: Path, text: str) -> Path:
    path = bundle / "connectors.yaml"
    path.write_text(text, encoding="utf-8")
    return path


def test_gate_naming_a_connectors_yaml_server_passes(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__kubernetes-admin__resources_create_or_update", '
        '"route": "sre-oncall"}]}}',
    )
    _write_connectors(
        bundle,
        "connectors:\n  kubernetes-admin:\n    image: ghcr.io/example/k8s-mcp:1.0.0\n",
    )

    result = validate_bundle(bundle)
    assert result.valid, result.errors


def test_connector_gate_in_the_plugin_form_is_rejected(tmp_path: Path) -> None:
    # The inverse of the case above, and the reason the two sources cannot share
    # one prefix: a connector is NOT plugin-namespaced at runtime, so the plugin
    # form arms a literal the SDK never produces -- green deploy, silent no-op.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__plugin_demo_kubernetes-admin__resources_create_or_update", '
        '"route": "sre-oncall"}]}}',
    )
    _write_connectors(
        bundle,
        "connectors:\n  kubernetes-admin:\n    image: ghcr.io/example/k8s-mcp:1.0.0\n",
    )

    assert _GATE_CODE in _codes(bundle)


def test_gate_naming_an_undeclared_connector_is_still_rejected(tmp_path: Path) -> None:
    # The new source widens the accepted set, it does not open it: a server named
    # by neither connectors.yaml nor the MCP config is still not namespaced.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__ghost__resources_create_or_update", "route": "sre-oncall"}]}}',
    )
    _write_connectors(
        bundle,
        "connectors:\n  kubernetes-admin:\n    image: ghcr.io/example/k8s-mcp:1.0.0\n",
    )

    result = validate_bundle(bundle)
    assert not result.valid
    messages = _gate_errors(bundle)
    assert len(messages) == 1, result.errors
    # Actionable: it names the connector form the author should have used.
    assert "mcp__kubernetes-admin__<tool>" in messages[0]


def test_both_surfaces_each_validate_under_their_own_prefix(tmp_path: Path) -> None:
    # A bundle with an MCP server AND a connector: each gate must use ITS source's
    # namespacing rule, and crossing them fails.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "mcpServers": {"crm": {"command": "crm-server"}}, '
        '"approvalPolicy": {"gates": ['
        '{"gate": "mcp__plugin_demo_crm__send_contract", "route": "legal"}, '
        '{"gate": "mcp__grafana__query", "route": "sre-oncall"}]}}',
    )
    _write_connectors(bundle, "connectors:\n  grafana:\n    url: https://mcp.internal/mcp\n")

    result = validate_bundle(bundle)
    assert result.valid, result.errors


def test_gate_check_stays_silent_when_connectors_yaml_is_unreadable(tmp_path: Path) -> None:
    # An unparseable connectors.yaml makes the accepted set unknowable. The file
    # error already fires; the gate check must not stack a second, misleading error
    # telling the author their correct gate names an undeclared server.
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "mcp__kubernetes-admin__resources_create_or_update", '
        '"route": "sre-oncall"}]}}',
    )
    _write_connectors(bundle, "connectors: [oops\n")

    codes = _codes(bundle)
    assert "connectors.unreadable" in codes
    assert _GATE_CODE not in codes


# --------------------------------------------------------------------------- #
# Bundle intake for a source-built connector -- ADR 0113
#
# `validate_bundle` is the ONE gate a bundle passes through whatever entry point
# it arrives by (apps/api/CLAUDE.md): the CLI upload, the console's create-agent
# modal, and the git push path all route through it. That is why these rules
# live here rather than in the API router, and why every assertion below goes
# through `validate_bundle` rather than through the lock model directly.
# --------------------------------------------------------------------------- #
_REGISTRY_IMAGE = (
    "ghcr.io/acme-corp/acme-bot-k8s-write-mcp@sha256:"
    "0000000000000000000000000000000000000000000000000000000000000000"
)
_LOCAL_IMAGE = "sha256:1111111111111111111111111111111111111111111111111111111111111111"

_BUILT_CONNECTORS = (
    "connectors:\n"
    "  k8s-write:\n"
    "    build:\n"
    "      context: connectors/k8s-write\n"
    "      platforms: [linux/amd64, linux/arm64]\n"
)


def _built_bundle(tmp_path: Path, connectors_yaml: str = _BUILT_CONNECTORS) -> Path:
    """A minimal bundle whose only connector is declared as source."""

    root = _bundle(tmp_path, '{"name": "acme-bot", "version": "0.1.0", "description": "t"}')
    (root / "skills" / "acme-bot").mkdir(parents=True, exist_ok=True)
    (root / "skills" / "acme-bot" / "SKILL.md").write_text(
        "---\nname: acme-bot\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    (root / "connectors.yaml").write_text(connectors_yaml, encoding="utf-8")
    context = root / "connectors" / "k8s-write"
    context.mkdir(parents=True, exist_ok=True)
    (context / "Dockerfile").write_text(
        "FROM scratch\nCOPY server.py /server.py\n", encoding="utf-8"
    )
    (context / "server.py").write_text("print('acme')\n", encoding="utf-8")
    return root


def _write_lock(
    root: Path,
    *,
    image: str = _REGISTRY_IMAGE,
    delivery: str = "registry",
    source_digest: str | None = None,
) -> None:
    """Write a well-formed lock for the bundle's one built connector."""

    from plugin_format import connector_lock
    from plugin_format.connectors import ConnectorBuild

    build = ConnectorBuild.model_validate(
        {"context": "connectors/k8s-write", "platforms": ["linux/amd64", "linux/arm64"]}
    )
    digest = source_digest or connector_lock.source_digest_of(
        root / "connectors" / "k8s-write", build
    )
    (root / connector_lock.CONNECTOR_LOCK_FILE).write_text(
        "version: 1\n"
        "connectors:\n"
        "  k8s-write:\n"
        f"    image: {image}\n"
        f"    delivery: {delivery}\n"
        "    platforms: [linux/amd64, linux/arm64]\n"
        f"    source_digest: {digest}\n",
        encoding="utf-8",
    )


def test_a_locked_build_bundle_validates(tmp_path: Path) -> None:
    # The control for every negative below. Without it a rule that rejected
    # every build: bundle outright would pass all of them.
    root = _built_bundle(tmp_path)
    _write_lock(root)
    result = validate_bundle(root)
    assert result.valid, [e.code for e in result.errors]


def test_a_build_bundle_with_no_lock_is_refused(tmp_path: Path) -> None:
    # gitflow.py creates the Version and stores the bundle only AFTER validation
    # passes, so this is what stops a lockless build: version from ever
    # existing. Without it the deployment goes active, the runner still derives
    # a hosted URL for the connector, and the reconciler retries forever against
    # something that was never built.
    root = _built_bundle(tmp_path)
    result = validate_bundle(root)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "connectors.lock_missing")
    assert "k8s-write" in issue.message


def test_an_image_bundle_with_no_lock_still_validates(tmp_path: Path) -> None:
    # Every rule this ticket adds is conditioned on a build: declaration, and no
    # bundle in the wild has one. A lock requirement that fired on the image:
    # form would reject every hosted-connector bundle that exists.
    root = _built_bundle(
        tmp_path,
        "connectors:\n  grafana:\n    image: ghcr.io/acme-corp/mcp-grafana:0.17.2\n",
    )
    assert validate_bundle(root).valid


def test_a_lock_missing_only_one_of_two_built_connectors_is_refused(tmp_path: Path) -> None:
    # Per connector, not per file. A lock that covers the first connector and
    # not the second would otherwise satisfy a file-level presence check while
    # the second renders `image: null`.
    root = _built_bundle(
        tmp_path,
        _BUILT_CONNECTORS
        + "  tempo:\n    build:\n      context: connectors/tempo\n      platforms: [linux/amd64]\n",
    )
    (root / "connectors" / "tempo").mkdir(parents=True, exist_ok=True)
    (root / "connectors" / "tempo" / "Dockerfile").write_text("FROM scratch\n", encoding="utf-8")
    _write_lock(root)
    result = validate_bundle(root)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "connectors.lock_missing")
    assert "tempo" in issue.message


def test_a_stale_lock_is_refused_when_the_source_changed(tmp_path: Path) -> None:
    # Without it, a git push after a source edit activates the PREVIOUS digest
    # and the deployed connector silently stops matching the reviewed source.
    # The recomputation is pure hashing over the already extracted tree -- no
    # docker, no registry, no network -- so the API stays a pure renderer.
    root = _built_bundle(tmp_path)
    _write_lock(root)
    assert validate_bundle(root).valid, "the control: unchanged source validates"

    (root / "connectors" / "k8s-write" / "server.py").write_text(
        "print('acme, but different')\n", encoding="utf-8"
    )
    result = validate_bundle(root)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "connectors.lock_stale")
    assert "k8s-write" in issue.message


def test_a_stale_lock_is_refused_when_only_the_declaration_changed(tmp_path: Path) -> None:
    # The second, independent path: not one byte of the build context changed,
    # only `platforms` in connectors.yaml. The previous lock names a
    # single-arch artifact that cannot run on half the nodes. A digest computed
    # over the tree alone would report this bundle as fresh.
    root = _built_bundle(tmp_path)
    _write_lock(root)
    (root / "connectors.yaml").write_text(
        _BUILT_CONNECTORS.replace("[linux/amd64, linux/arm64]", "[linux/amd64]"), encoding="utf-8"
    )
    result = validate_bundle(root)
    assert not result.valid
    assert "connectors.lock_stale" in {i.code for i in result.errors}


def test_a_valid_local_daemon_lock_passes_intake(tmp_path: Path) -> None:
    # The regression pin for review round 2 finding r2-3, and it is a pin
    # against a rule being ADDED rather than removed. `curie local deploy`
    # legitimately uploads a bundle carrying a local-daemon lock; the
    # registry-only rule belongs to the cluster deploy preflight, which is the
    # only path that needs an artifact a Kubernetes node can pull. Making
    # intake refuse local-daemon turns this test red, which is the point.
    root = _built_bundle(tmp_path)
    _write_lock(root, image=_LOCAL_IMAGE, delivery="local-daemon")
    result = validate_bundle(root)
    assert result.valid, [e.code for e in result.errors]


def test_a_lock_carrying_a_tag_is_refused_at_intake(tmp_path: Path) -> None:
    # A hand-edited lock reaches intake exactly as a generated one does. The
    # mutable-tag refusal has to fire here too, not only at render time, or a
    # version whose connector can never be rendered is stored and activated.
    root = _built_bundle(tmp_path)
    _write_lock(root, image="ghcr.io/acme-corp/acme-bot-k8s-write-mcp:v1")
    result = validate_bundle(root)
    assert not result.valid
    assert any(i.code.startswith("connectors.lock") for i in result.errors)


def test_a_malformed_lock_is_refused_at_intake(tmp_path: Path) -> None:
    # Rejected here rather than stored and hit at render time, where the failure
    # is an opaque parse error inside the API on a version that already looks
    # deployed.
    root = _built_bundle(tmp_path)
    (root / "connectors.lock.yaml").write_text(
        "version: 1\nconnectors:\n  k8s-write:\n    image: [unclosed\n", encoding="utf-8"
    )
    result = validate_bundle(root)
    assert not result.valid
    assert any(i.code.startswith("connectors.lock") for i in result.errors)


def test_a_lock_beside_no_connectors_file_is_still_validated(tmp_path: Path) -> None:
    # A leftover lock with no connectors.yaml at all is a bundle that lost its
    # declaration. Ignoring the file entirely would let a malformed one ride
    # along into the stored artifact unread.
    root = _bundle(tmp_path, '{"name": "acme-bot", "version": "0.1.0", "description": "t"}')
    (root / "skills" / "acme-bot").mkdir(parents=True, exist_ok=True)
    (root / "skills" / "acme-bot" / "SKILL.md").write_text(
        "---\nname: acme-bot\ndescription: t\n---\nhi\n", encoding="utf-8"
    )
    (root / "connectors.lock.yaml").write_text("version: 1\nconnectors: nope\n", encoding="utf-8")
    result = validate_bundle(root)
    assert not result.valid
    assert any(i.code.startswith("connectors.lock") for i in result.errors)


def test_a_build_connector_with_a_missing_context_dir_is_refused(tmp_path: Path) -> None:
    # A declared context that does not exist in the bundle is a malformed
    # bundle, and it must be refused HERE rather than skipped. Skipping it makes
    # the lock rules fail open: the staleness recomputation has no tree to hash,
    # so the lockless and stale refusals silently do not apply, the version is
    # created, the deployment goes active, and the first thing anyone learns is
    # a render-time failure on a connector nobody could have built. Refusing at
    # intake is the same reason every other rule in this file lives here.
    root = _built_bundle(tmp_path)
    _write_lock(root)
    shutil.rmtree(root / "connectors" / "k8s-write")

    result = validate_bundle(root)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "connectors.build_context_missing")
    assert "k8s-write" in issue.message
    assert "connectors/k8s-write" in issue.message, "name the path the author declared"


def test_the_same_bundle_with_the_context_present_validates(tmp_path: Path) -> None:
    # The sibling positive, and the thing that stops the refusal above from
    # being satisfiable by rejecting every build: bundle outright. Identical in
    # every respect except that the declared directory exists.
    root = _built_bundle(tmp_path)
    _write_lock(root)
    assert (root / "connectors" / "k8s-write").is_dir()
    result = validate_bundle(root)
    assert result.valid, [e.code for e in result.errors]


def test_a_build_context_symlinked_out_of_the_bundle_is_refused(tmp_path: Path) -> None:
    # RED before the fix: `connectors.build_context_escapes` is absent from
    # result.errors and the bundle validates, because intake joined the declared
    # path onto the root and asked only `is_dir()`. The declaration is textually
    # clean, so `_escapes` cannot see this, and `safe_extract` guards a different
    # entry point (an uploaded archive) than the gitflow path, which hands
    # validate_bundle a real tree.
    #
    # What that bought an author: `source_digest_of` hashes whatever tree the
    # context names, so a symlink makes the pinned digest a function of bytes
    # outside the bundle -- content no reviewer of this bundle ever saw, and
    # content that can change under the lock without the lock going stale. The
    # CLI's `resolve_context` has refused exactly this since day one, so intake
    # was the more permissive of the two gates on the same rule.
    root = _built_bundle(tmp_path)
    _write_lock(root)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "Dockerfile").write_text("FROM scratch\nRUN whoami\n", encoding="utf-8")
    shutil.rmtree(root / "connectors" / "k8s-write")
    (root / "connectors" / "k8s-write").symlink_to(outside, target_is_directory=True)

    result = validate_bundle(root)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "connectors.build_context_escapes")
    assert "k8s-write" in issue.message


# --- toolPolicy: the vanilla MCP tool policy, checked at deploy ----------------
#
# The policy classifies a canonical "<server>/<tool>" name as allow /
# approval-required / deny, with DENY as the default for anything unmatched. Every
# rule below exists because the failure it prevents is silent: a malformed pattern
# never matches, so a bundle validates green while the rule its author wrote does
# nothing at all. These go through the public validate_bundle, never a private
# helper, because that is the one gate every intake path shares.
#
# validate_bundle carries an ENFORCEMENT HANDSHAKE: a caller states the tool-policy
# contract it implements via `enforces_tool_policy`, and a bundle declaring a
# toolPolicy is REFUSED (tool_policy.unenforced) by any caller that does not state
# the supported id. That is the fail-closed property -- apps/api's bundles.py and
# runner's plugin.py both call validate_bundle(root) with no argument today, so a
# policy-carrying bundle is refused by both until their lane actually enforces it,
# rather than being accepted and silently ignored.

_TP_ENFORCEMENT = "curie/mcp-tool-policy@1"
_UNENFORCED = "tool_policy.unenforced"


def _tool_policy_bundle(tmp_path: Path, policy: str) -> Path:
    """A minimal bundle whose manifest carries the given toolPolicy JSON object."""
    return _bundle(tmp_path, '{"name": "demo", "toolPolicy": ' + policy + "}")


def _tool_policy_codes(bundle: Path) -> list[str]:
    """The tool_policy.* error codes seen by a caller that DOES enforce the contract.

    Every declared-policy case below states enforcement, so each one proves its
    OWN error code fires. Without that the blanket tool_policy.unenforced error
    would mask every other check and the whole table would pass vacuously.
    """
    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    return [i.code for i in result.errors if i.code.startswith("tool_policy.")]


# --- the enforcement handshake: a declared policy nobody enforces is refused ---


def test_bundle_without_a_tool_policy_validates_green_under_the_default_call(
    tmp_path: Path,
) -> None:
    """BACKWARD COMPATIBILITY control: absence is never a tool_policy issue.

    Every bundle in the wild today has no toolPolicy, and today's callers
    (apps/api bundles.py, runner plugin.py) pass no enforcement argument. Such a
    bundle must return the SAME valid/errors/warnings it did before the field
    existed: green, with no tool_policy.* issue of any kind. The new optional
    field does surface as ``toolPolicy: None`` in ``PluginManifest.model_dump()``
    -- the ordinary new-optional-field patch behaviour, and not something the
    validator's result reflects.
    """

    bundle = _bundle(tmp_path, '{"name": "demo"}')
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle)
    assert result.valid, result.errors
    assert not [i for i in result.errors if i.code.startswith("tool_policy.")]
    assert not [i for i in result.warnings if i.code.startswith("tool_policy.")]


def test_a_consumer_declaring_no_enforcement_refuses_a_declared_policy(tmp_path: Path) -> None:
    """The default call REFUSES a bundle that ships a toolPolicy. This is the whole point.

    A consumer that has not declared which tool-policy contract it implements
    cannot silently accept a bundle carrying one: it would load the agent, ignore
    the policy, and give the author a control that exists only on paper. Both
    call sites in the tree (apps/api bundles.py:101, runner plugin.py:60) pass no
    argument today, so both refuse such a bundle until their lane enforces it.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", "allow": ["grafana/list_datasources"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == _UNENFORCED)
    # Actionable: the caller must be told which contract id it has to implement.
    assert _TP_ENFORCEMENT in issue.message


def test_the_same_declared_policy_validates_once_enforcement_is_declared(tmp_path: Path) -> None:
    """The positive half of the handshake: the identical bundle passes for a real enforcer.

    Paired with the test above on purpose -- same bundle, only the caller's
    declaration differs -- so the refusal is proven to come from the handshake and
    not from anything wrong with the policy itself.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", "allow": ["grafana/list_datasources"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert result.valid, result.errors
    assert not [i for i in result.errors if i.code.startswith("tool_policy.")]


def test_a_consumer_enforcing_a_different_contract_refuses_a_declared_policy(
    tmp_path: Path,
) -> None:
    """A caller implementing @2 cannot enforce an @1 policy, so the bundle is refused.

    "Some enforcement" is not enforcement of THIS contract; accepting a near-miss
    id would apply rules the author never wrote.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", "allow": ["grafana/list_datasources"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy="curie/mcp-tool-policy@2")
    assert not result.valid
    assert _UNENFORCED in {i.code for i in result.errors}


def test_the_handshake_error_is_the_declared_id_pinned_to_the_exported_constant() -> None:
    """The id the validator demands is the one the module exports; not two literals."""

    assert TOOL_POLICY_ENFORCEMENT == _TP_ENFORCEMENT


# --- shape ---------------------------------------------------------------------


@pytest.mark.parametrize("literal", ['["grafana/*"]', '"allow everything"'], ids=["list", "string"])
def test_tool_policy_that_is_not_an_object_is_rejected(tmp_path: Path, literal: str) -> None:
    """A list or string toolPolicy carries no collections, so it can classify nothing.

    It must be rejected rather than ignored: a manifest that "has a toolPolicy"
    which the loader cannot read is the worst outcome -- the author sees the key
    in their bundle and assumes it is in force.

    ``PluginManifest.toolPolicy`` is typed ``dict[str, Any] | None``, the same as
    ``approvalPolicy``, so a non-object value fails manifest model validation one
    layer before ``_validate_tool_policy`` ever runs. The code is therefore
    ``manifest.invalid``, not ``tool_policy.invalid`` -- but the property under
    test, rejection, still holds, so this asserts the reported error names
    ``toolPolicy`` rather than a specific error code.
    """

    bundle = _tool_policy_bundle(tmp_path, literal)

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert not result.valid
    assert any("toolPolicy" in issue.message for issue in result.errors), result.errors


def test_tool_policy_collection_that_is_not_a_list_is_rejected(tmp_path: Path) -> None:
    """``allow`` given as a bare string is a shape error, not a one-element list.

    Coercing it would silently reinterpret the author's intent; a string is also
    iterable, so a lenient implementation could end up matching per-character.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", "allow": "grafana/*"}',
    )

    assert "tool_policy.invalid" in _tool_policy_codes(bundle)
    assert not validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT).valid


def test_a_misspelled_collection_key_is_rejected_not_silently_dropped(tmp_path: Path) -> None:
    """``denny`` instead of ``deny`` is a DENY-BYPASS, so ToolPolicy forbids extra keys.

    The attack this closes: with a lenient model the misspelled key is accepted
    and discarded, the real ``deny`` list is empty, and ``k8s/delete_namespace``
    -- which the author believes is blocked -- falls through to the ``allow`` glob
    and becomes callable. A typo silently WIDENS permissions.

    This is why ToolPolicy is strict where PluginManifest is lenient: the manifest
    has an external producer (Claude Code) that legitimately adds keys; a
    Curie-owned policy object has none, so an unknown key is always a mistake --
    the same reasoning ConnectorSpec, ConnectorLockEntry and DeployTarget already
    encode with ``extra="forbid"``.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"allow": ["k8s/*"], "denny": ["k8s/delete_namespace"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"k8s": {"command": "k8s-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "tool_policy.invalid")
    assert "denny" in issue.message


def test_manifest_stays_lenient_around_a_strict_tool_policy(tmp_path: Path) -> None:
    """Strictness is scoped to the ToolPolicy object; the manifest keeps accepting extras.

    Tightening the whole manifest would reject real Claude Code bundles carrying
    fields this package does not model -- the compatibility wedge.
    """

    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "futureField": 42, "toolPolicy": '
        '{"enforcement": "' + _TP_ENFORCEMENT + '", "allow": ["grafana/*"]}}',
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert result.valid, result.errors


# --- the enforcement id the BUNDLE declares -----------------------------------


def test_tool_policy_without_an_enforcement_id_is_rejected(tmp_path: Path) -> None:
    """A policy that names no enforcement contract cannot be applied by any build."""

    bundle = _tool_policy_bundle(tmp_path, '{"allow": ["grafana/*"]}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "tool_policy.enforcement_unsupported")
    assert _TP_ENFORCEMENT in issue.message


def test_tool_policy_with_a_blank_enforcement_id_is_rejected(tmp_path: Path) -> None:
    """An empty string is not a contract id; it must not read as "unset, so fine"."""

    bundle = _tool_policy_bundle(tmp_path, '{"enforcement": "", "allow": ["grafana/*"]}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "tool_policy.enforcement_unsupported")
    assert _TP_ENFORCEMENT in issue.message


@pytest.mark.parametrize(
    "declared",
    [
        pytest.param(" curie/mcp-tool-policy@1", id="leading-space"),
        pytest.param("curie/mcp-tool-policy@1 ", id="trailing-space"),
        pytest.param("curie/mcp-tool-policy\t@1", id="internal-tab"),
    ],
)
def test_a_whitespace_padded_enforcement_id_is_rejected(tmp_path: Path, declared: str) -> None:
    """The id is compared BYTE-FOR-BYTE: padding makes it a different string, so it is refused.

    Not stripping is the fail-CLOSED choice. Accepting " curie/mcp-tool-policy@1 "
    as v1 would silently decide on the author's behalf that the padding meant
    nothing -- and would put the deploy validator and any future runtime loader
    one normalization apart, which is the #453/#544 drift. ``load_tool_policy``
    refuses the same strings for the same reason.

    Deliberately unlike the approval-gate check, which DOES strip gate names so
    the validator and the loader agree on one tool name: a version discriminator
    is not a tool name.
    """

    # json.dumps, not string concatenation: the tab case carries a real control
    # character, which is illegal RAW inside a JSON string and would otherwise
    # fail manifest parsing instead of reaching the enforcement check.
    bundle = _tool_policy_bundle(
        tmp_path,
        json.dumps({"enforcement": declared, "allow": ["grafana/*"]}),
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "tool_policy.enforcement_unsupported")
    # Both what was written and what this build implements, or the fix is a guess.
    assert repr(declared) in issue.message
    assert _TP_ENFORCEMENT in issue.message


def test_tool_policy_with_a_future_enforcement_id_is_rejected(tmp_path: Path) -> None:
    """A bundle asking for @2 semantics gets refused, not silently given @1 semantics.

    Asserted with the caller passing the SUPPORTED id, so this is not the
    handshake error in disguise: the bundle's own declaration is what fails. The
    message must name BOTH ids -- what the author asked for and what this build
    implements -- or the fix is a guess.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "curie/mcp-tool-policy@2", "allow": ["grafana/*"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "tool_policy.enforcement_unsupported")
    assert "curie/mcp-tool-policy@2" in issue.message
    assert _TP_ENFORCEMENT in issue.message


# --- pattern grammar ----------------------------------------------------------


def test_valid_tool_policy_over_declared_servers_validates_green(tmp_path: Path) -> None:
    """The happy path: well-formed patterns naming servers the bundle actually declares."""

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"allow": ["grafana/list_datasources", "grafana/query_*"], '
        '"approvalRequired": ["kubernetes/pods_*"], '
        '"deny": ["kubernetes/resources_delete"]}',
    )
    _write_mcp(
        bundle,
        '{"mcpServers": {"grafana": {"command": "grafana-mcp"}, '
        '"kubernetes": {"command": "k8s-mcp"}}}',
    )

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert result.valid, result.errors
    assert _tool_policy_codes(bundle) == []


def test_malformed_patterns_are_reported_in_every_collection(tmp_path: Path) -> None:
    """All THREE collections are pattern-checked, each at its own location.

    The realistic defect is wiring the check into ``allow`` only: a malformed
    pattern in ``deny`` never matches anything, so the bundle validates green while
    the author believes a tool is blocked. One bad pattern per collection proves
    each is walked.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"allow": ["grafana/**"], '
        '"approvalRequired": ["kubernetes/pods run"], '
        '"deny": ["a/b/c"]}',
    )
    _write_mcp(
        bundle,
        '{"mcpServers": {"grafana": {"command": "grafana-mcp"}, '
        '"kubernetes": {"command": "k8s-mcp"}}}',
    )

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert not result.valid
    issues = [i for i in result.errors if i.code == "tool_policy.pattern_invalid"]
    assert len(issues) == 3, result.errors
    locations = [i.location for i in issues]
    for collection in ("allow", "approvalRequired", "deny"):
        assert any(collection in loc and "[0]" in loc for loc in locations), locations
    # And each offending pattern is named, so the author can find it.
    messages = " ".join(i.message for i in issues)
    assert "grafana/**" in messages
    assert "a/b/c" in messages


def test_pattern_invalid_location_names_the_collection_and_index(tmp_path: Path) -> None:
    """The second entry of a collection is reported at index 1, not 0.

    A hardcoded index would send the author to the wrong line of a long list.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"allow": ["grafana/list_datasources", "grafana/[abc]*"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    issue = next(i for i in result.errors if i.code == "tool_policy.pattern_invalid")
    assert "allow" in issue.location
    assert "[1]" in issue.location


@pytest.mark.parametrize(
    "pattern",
    ["grafana/**", "grafana/[abc]*", "a/b/c", "grafana/tool!", "list_datasources"],
)
def test_the_validator_rejects_exactly_what_validate_pattern_rejects(
    tmp_path: Path, pattern: str
) -> None:
    """ONE normalization path: the deploy gate applies the exported grammar, not its own.

    The #453/#544 lesson -- a validator and a runtime loader that each normalize
    separately drift apart silently and ship a fail-open. Both halves of this test
    use the SAME pattern string, so a second, divergent grammar inside validate.py
    fails here instead of at turn time.
    """

    assert validate_pattern(pattern) is not None, "fixture must be an invalid pattern"

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", "deny": ["' + pattern + '"]}',
    )

    codes = _tool_policy_codes(bundle)
    assert "tool_policy.pattern_invalid" in codes


def test_duplicate_pattern_within_one_collection_is_rejected(tmp_path: Path) -> None:
    """The same pattern twice in one collection is a copy-paste artifact, not a rule.

    It is rejected rather than deduped because the author almost certainly meant
    to write two DIFFERENT patterns, and silently collapsing them hides the tool
    they thought they had covered.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"allow": ["grafana/query_*", "grafana/list_*", "grafana/query_*"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "tool_policy.pattern_duplicate")
    assert "grafana/query_*" in issue.message


def test_identical_pattern_in_allow_and_deny_is_rejected(tmp_path: Path) -> None:
    """One pattern string in two collections is a contradiction the author must resolve.

    Precedence would silently resolve it (deny wins), which is exactly why it must
    be rejected: the author wrote both, so one of the two is a mistake, and the
    validator cannot know which.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"allow": ["kubernetes/resources_scale"], '
        '"deny": ["kubernetes/resources_scale"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"kubernetes": {"command": "k8s-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "tool_policy.pattern_conflict")
    assert "kubernetes/resources_scale" in issue.message


def test_overlapping_but_different_patterns_across_collections_validate_green(
    tmp_path: Path,
) -> None:
    """OVERLAP is legal and deliberate; only exact duplication across collections is not.

    ``deny: k8s/resources_*`` beside ``allow: k8s/resources_scale`` is the normal
    way to carve an exception out of a broad rule, and precedence gives it a
    well-defined answer. A conflict check that compared by MATCH rather than by
    exact string would reject this idiom and make the policy unusable.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"allow": ["k8s/resources_scale"], '
        '"deny": ["k8s/resources_*"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"k8s": {"command": "k8s-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert result.valid, result.errors
    assert _tool_policy_codes(bundle) == []


# --- the declared-server cross-check ------------------------------------------


def test_pattern_naming_an_undeclared_server_is_rejected(tmp_path: Path) -> None:
    """A literal server segment the bundle never declares is a typo that gates nothing.

    The pattern is syntactically fine, so nothing else catches it: the rule simply
    never matches a live tool, and the author believes they wrote a control.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", "deny": ["grafna/delete_dashboard"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert not result.valid
    issue = next(i for i in result.errors if i.code == "tool_policy.unknown_server")
    assert "grafna" in issue.message


def test_a_wildcard_server_segment_never_triggers_unknown_server(tmp_path: Path) -> None:
    """``*/pods_run`` names no server, so there is nothing to cross-check.

    Asserted on a bundle that declares NO servers at all, the case where a naive
    implementation ("is the segment in the declared set?") would reject every
    wildcard pattern.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"approvalRequired": ["*/pods_run"], "allow": ["*/*"]}',
    )

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert result.valid, result.errors
    assert "tool_policy.unknown_server" not in _tool_policy_codes(bundle)


def test_a_literal_segment_naming_a_declared_server_does_not_trigger_unknown_server(
    tmp_path: Path,
) -> None:
    """The positive control for the cross-check: a declared server is accepted."""

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", "allow": ["grafana/list_datasources"]}',
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert result.valid, result.errors
    assert "tool_policy.unknown_server" not in _tool_policy_codes(bundle)


def test_a_connectors_yaml_server_satisfies_the_cross_check(tmp_path: Path) -> None:
    """connectors.yaml is the bundle's OTHER tool surface, so its servers count too.

    Without it, a bundle whose whole surface is connectors would have every literal
    pattern rejected and could not write a policy at all.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"approvalRequired": ["kubernetes-admin/resources_create_or_update"]}',
    )
    _write_connectors(
        bundle,
        "connectors:\n  kubernetes-admin:\n    image: ghcr.io/example/k8s-mcp:1.0.0\n",
    )

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert result.valid, result.errors
    assert "tool_policy.unknown_server" not in _tool_policy_codes(bundle)


def test_unknown_server_check_stays_silent_when_the_mcp_declaration_is_unreadable(
    tmp_path: Path,
) -> None:
    """An unreadable .mcp.json makes the declared-server set UNKNOWABLE, not empty.

    Treating it as empty would report every literal pattern as naming an undeclared
    server, stacking a pile of misleading errors on top of the real JSON error and
    sending the author to fix the wrong file. The MCP error must fire alone.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"allow": ["grafana/list_datasources"], "deny": ["kubernetes/pods_run"]}',
    )
    _write_mcp(bundle, "{not json")

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    codes = {i.code for i in result.errors}
    assert "mcp.invalid_json" in codes
    assert "tool_policy.unknown_server" not in codes


def test_a_policy_denying_everything_warns_but_still_validates(tmp_path: Path) -> None:
    """Three empty collections deny every tool: coherent, so a WARNING, not an error.

    It is legal (a deliberate lockdown) but almost always a mistake, and the agent
    that ships it will simply refuse every tool call with no explanation at turn
    time. Warn loudly at deploy, where the author is still looking.
    """

    bundle = _tool_policy_bundle(
        tmp_path,
        '{"enforcement": "' + _TP_ENFORCEMENT + '", '
        '"allow": [], "approvalRequired": [], "deny": []}',
    )
    _write_mcp(bundle, '{"mcpServers": {"grafana": {"command": "grafana-mcp"}}}')

    result = validate_bundle(bundle, enforces_tool_policy=TOOL_POLICY_ENFORCEMENT)
    assert result.valid, result.errors
    assert "tool_policy.denies_everything" in {i.code for i in result.warnings}


# --- two conformance profiles (ADR-0135) -------------------------------------
#
# One validator, two profiles. `claude-plugin` is the INGESTION contract and
# keeps today's leniency exactly -- every specification divergence is a WARNING,
# so widening the accepted shapes can never break a deploy. `agent-skills-strict`
# is a PUBLISHABILITY gate: the same findings are errors, because a bundle the
# reference validator rejects must not clear it.
#
# Every row below is asserted under BOTH profiles. The expected verdicts are the
# spike's confirmed fixture verdicts, not our reading of the spec prose.

_PLUGIN = "claude-plugin"
_STRICT = "agent-skills-strict"
_SPEC = "skill.spec_nonconformant"

_BLOCK_LIST = f"{_SPEC}.allowed_tools_block_list"
_FLOW_LIST = f"{_SPEC}.allowed_tools_flow_list"
_UNSERIALIZABLE = f"{_SPEC}.allowed_tools_unserializable"
_UNKNOWN_FIELD = f"{_SPEC}.unknown_field"
_NAME_LENGTH = f"{_SPEC}.name_length"
_NAME_CHARSET = f"{_SPEC}.name_charset"
_NAME_DIR_MISMATCH = f"{_SPEC}.name_dir_mismatch"
_DESCRIPTION_LENGTH = f"{_SPEC}.description_length"
_COMPATIBILITY_LENGTH = f"{_SPEC}.compatibility_length"
_METADATA_SHAPE = f"{_SPEC}.metadata_shape"
_LICENSE_TYPE = f"{_SPEC}.license_type"


def _write_spec_skill(bundle: Path, frontmatter: str, *, dir_name: str = "demo") -> Path:
    """Write ``skills/<dir_name>/SKILL.md`` with a VERBATIM frontmatter body.

    Distinct from ``_write_skill`` above, which prepends a fixed name and
    description. The conformance matrix has to vary both of those (name length,
    name charset, description length) and has to control the skill DIRECTORY name
    independently of the declared ``name`` for the dir-mismatch row, so it needs
    a helper that writes the frontmatter exactly as given.
    """
    skill = bundle / "skills" / dir_name / "SKILL.md"
    skill.parent.mkdir(parents=True, exist_ok=True)
    skill.write_text(f"---\n{frontmatter}---\n\n# Demo\n", encoding="utf-8")
    return skill


def _spec_codes(result: ValidationResult) -> tuple[set[str], set[str]]:
    """The ``skill.spec_nonconformant.*`` codes, split into (errors, warnings).

    Filtered to this prefix so an unrelated pre-existing finding cannot make an
    exact-set assertion brittle, while the exact set keeps the assertion strong
    enough to catch a check that fires twice or fires on the wrong row.
    """
    errors = {i.code for i in result.errors if i.code.startswith(_SPEC)}
    warnings = {i.code for i in result.warnings if i.code.startswith(_SPEC)}
    return errors, warnings


_D = "description: A demo skill.\n"

# (frontmatter, dir_name, plugin_warnings, strict_valid, strict_errors, strict_warnings)
_MATRIX = [
    pytest.param(
        f"name: demo\n{_D}allowed-tools:\n  - Bash\n",
        "demo",
        {_BLOCK_LIST},
        True,
        set(),
        {_BLOCK_LIST},
        id="block-list",
    ),
    pytest.param(
        f"name: demo\n{_D}allowed-tools: [Read, Bash]\n",
        "demo",
        {_FLOW_LIST},
        False,
        {_FLOW_LIST},
        set(),
        id="flow-list",
    ),
    pytest.param(
        f"name: demo\n{_D}allowed-tools: []\n",
        "demo",
        {_FLOW_LIST},
        False,
        {_FLOW_LIST},
        set(),
        id="empty-flow-list",
    ),
    pytest.param(
        # F3: a flow sequence opening on the line AFTER the key. A single-line
        # detector reads this as a block list, which is only a warning under the
        # strict profile -- a false PASS on a publishability gate.
        f"name: demo\n{_D}allowed-tools:\n  [Read]\n",
        "demo",
        {_FLOW_LIST},
        False,
        {_FLOW_LIST},
        set(),
        id="multiline-flow-list",
    ),
    pytest.param(
        # The headline fix. Today this is a hard `skill.frontmatter_invalid`
        # error ("Input should be a valid list") on the ingestion path.
        f'name: demo\n{_D}allowed-tools: "Read Bash"\n',
        "demo",
        set(),
        True,
        set(),
        set(),
        id="space-separated-string",
    ),
    pytest.param(
        f'name: demo\n{_D}allowed-tools: "Read,Bash"\n',
        "demo",
        set(),
        True,
        set(),
        set(),
        id="comma-separated-string",
    ),
    pytest.param(
        f'name: demo\n{_D}allowed-tools: ""\n',
        "demo",
        set(),
        True,
        set(),
        set(),
        id="empty-string",
    ),
    pytest.param(
        # Edge case 5: null parses to None, so the style is "absent", NOT
        # "string", and no allowed-tools finding fires at all.
        f"name: demo\n{_D}allowed-tools: null\n",
        "demo",
        set(),
        True,
        set(),
        set(),
        id="null-allowed-tools",
    ),
    pytest.param(
        # A real Claude Code first-class field that the specification's closed
        # world does not contain.
        f"name: demo\n{_D}disable-model-invocation: true\n",
        "demo",
        {_UNKNOWN_FIELD},
        False,
        {_UNKNOWN_FIELD},
        set(),
        id="disable-model-invocation",
    ),
    pytest.param(
        f"name: demo\n{_D}x-custom-field: anything\n",
        "demo",
        {_UNKNOWN_FIELD},
        False,
        {_UNKNOWN_FIELD},
        set(),
        id="unknown-field",
    ),
    pytest.param(
        f"name: demo\n{_D}metadata:\n  team: sales\n",
        "demo",
        set(),
        True,
        set(),
        set(),
        id="metadata-str-to-str",
    ),
    pytest.param(
        # D2: `metadata` is validated off the RAW dict by the strict profile, so
        # the lenient model never hard-errors on it.
        f"name: demo\n{_D}metadata:\n  retries: 3\n",
        "demo",
        {_METADATA_SHAPE},
        False,
        {_METADATA_SHAPE},
        set(),
        id="metadata-non-string-value",
    ),
    pytest.param(
        f"name: demo\n{_D}license: 3\n",
        "demo",
        {_LICENSE_TYPE},
        False,
        {_LICENSE_TYPE},
        set(),
        id="license-not-a-string",
    ),
    pytest.param(
        f"name: demo\n{_D}compatibility: {'c' * 501}\n",
        "demo",
        {_COMPATIBILITY_LENGTH},
        False,
        {_COMPATIBILITY_LENGTH},
        set(),
        id="compatibility-over-500",
    ),
    pytest.param(
        # Edge case 10: the rule compares against the IMMEDIATE parent directory.
        f"name: demo\n{_D}",
        "not-demo",
        {_NAME_DIR_MISMATCH},
        False,
        {_NAME_DIR_MISMATCH},
        set(),
        id="name-directory-mismatch",
    ),
    pytest.param(
        f"name: {'a' * 65}\n{_D}",
        "a" * 65,
        {_NAME_LENGTH},
        False,
        {_NAME_LENGTH},
        set(),
        id="name-over-64-chars",
    ),
    pytest.param(
        # Consecutive hyphens: the charset rule allows only SINGLE internal ones.
        f"name: a--b\n{_D}",
        "a--b",
        {_NAME_CHARSET},
        False,
        {_NAME_CHARSET},
        set(),
        id="name-consecutive-hyphens",
    ),
    pytest.param(
        f"name: demo\ndescription: {'d' * 1025}\n",
        "demo",
        {_DESCRIPTION_LENGTH},
        False,
        {_DESCRIPTION_LENGTH},
        set(),
        id="description-over-1024",
    ),
    pytest.param(
        f'name: demo\n{_D}allowed-tools: "Bash(git:*) Read"\n',
        "demo",
        set(),
        True,
        set(),
        set(),
        id="specifier-string-form",
    ),
    pytest.param(
        f'name: demo\n{_D}allowed-tools:\n  - "Bash(git:*)"\n',
        "demo",
        {_BLOCK_LIST},
        True,
        set(),
        {_BLOCK_LIST},
        id="specifier-list-form",
    ),
    pytest.param(
        # D4, the correction to an earlier draft: the splitter is paren-aware, so
        # whitespace INSIDE the specifier round-trips and this is NOT flagged as
        # unserializable. A paren-blind splitter reports it and would refuse a
        # perfectly legal Claude Code permission rule.
        f'name: demo\n{_D}allowed-tools: "Bash(git commit:*) Read"\n',
        "demo",
        set(),
        True,
        set(),
        set(),
        id="paren-with-space-string-form",
    ),
    pytest.param(
        f'name: demo\n{_D}allowed-tools:\n  - "Bash(git commit:*)"\n',
        "demo",
        {_BLOCK_LIST},
        True,
        set(),
        {_BLOCK_LIST},
        id="paren-with-space-list-form",
    ),
    pytest.param(
        # Unbalanced parens: serializable on its own, lossy in a list, so it is
        # named by a per-entry predicate rather than by the round-trip alone.
        f'name: demo\n{_D}allowed-tools:\n  - "Bash(git"\n',
        "demo",
        {_BLOCK_LIST, _UNSERIALIZABLE},
        False,
        {_UNSERIALIZABLE},
        {_BLOCK_LIST},
        id="unbalanced-paren-entry",
    ),
    pytest.param(
        f'name: demo\n{_D}allowed-tools:\n  - "Read,Write"\n',
        "demo",
        {_BLOCK_LIST, _UNSERIALIZABLE},
        False,
        {_UNSERIALIZABLE},
        {_BLOCK_LIST},
        id="depth-zero-comma-entry",
    ),
    pytest.param(
        f'name: demo\n{_D}allowed-tools:\n  - "Read Write"\n',
        "demo",
        {_BLOCK_LIST, _UNSERIALIZABLE},
        False,
        {_UNSERIALIZABLE},
        {_BLOCK_LIST},
        id="depth-zero-whitespace-entry",
    ),
    pytest.param(
        # Whitespace SURROUNDING an otherwise-clean entry: `parse_allowed_tools`
        # strips a list entry's surrounding whitespace before this check used to
        # see it, so `" Read "` looked clean even though it does not survive the
        # canonical `parse_allowed_tools(" ".join(entries)) == entries`
        # round-trip -- it normalizes to `"Read"`. The check now reads the
        # AUTHORED entry, matching the Rust `round_trips_in_allowed_tools` check.
        f'name: demo\n{_D}allowed-tools:\n  - " Read "\n',
        "demo",
        {_BLOCK_LIST, _UNSERIALIZABLE},
        False,
        {_UNSERIALIZABLE},
        {_BLOCK_LIST},
        id="surrounding-whitespace-entry",
    ),
]


@pytest.mark.parametrize(
    ("frontmatter", "dir_name", "plugin_warnings", "_sv", "_se", "_sw"), _MATRIX
)
def test_claude_plugin_profile_keeps_every_shape_valid(
    tmp_path: Path,
    frontmatter: str,
    dir_name: str,
    plugin_warnings: set[str],
    _sv: bool,
    _se: set[str],
    _sw: set[str],
) -> None:
    """The ingestion contract: a nonconformance is a WARNING and never blocks.

    This half of the matrix is the leniency mandate made executable. If any row
    here turns into an error, a bundle that deploys today stops deploying --
    which is the regression `packages/CLAUDE.md` forbids outright.
    """
    bundle = _bundle(tmp_path, '{"name": "demo"}')
    _write_spec_skill(bundle, frontmatter, dir_name=dir_name)

    result = validate_bundle(bundle, profile=_PLUGIN)
    assert result.valid, result.errors
    errors, warnings = _spec_codes(result)
    assert errors == set(), "a conformance finding must never be an error here"
    assert warnings == plugin_warnings


@pytest.mark.parametrize(
    ("frontmatter", "dir_name", "_pw", "strict_valid", "strict_errors", "strict_warnings"), _MATRIX
)
def test_agent_skills_strict_profile_enforces_the_specification(
    tmp_path: Path,
    frontmatter: str,
    dir_name: str,
    _pw: set[str],
    strict_valid: bool,
    strict_errors: set[str],
    strict_warnings: set[str],
) -> None:
    """The publishability gate: the same findings, promoted to errors.

    The one deliberate exception is the block list, which stays a WARNING even
    here because the reference validator accepts it while the spec prose names
    the string canonical. Erroring on it would fail bundles skills-ref passes.
    """
    bundle = _bundle(tmp_path, '{"name": "demo"}')
    _write_spec_skill(bundle, frontmatter, dir_name=dir_name)

    result = validate_bundle(bundle, profile=_STRICT)
    errors, warnings = _spec_codes(result)
    assert result.valid is strict_valid, (errors, warnings)
    assert errors == strict_errors
    assert warnings == strict_warnings


def test_the_default_profile_is_claude_plugin(tmp_path: Path) -> None:
    """The ingestion default is the lenient profile, and it is the DEFAULT.

    A caller that passes nothing must get today's behavior. Both production
    callers do exactly that, so this is the guarantee that keeps runner boot and
    deploy ingestion source-compatible.
    """
    from plugin_format import PROFILE_AGENT_SKILLS_STRICT, PROFILE_CLAUDE_PLUGIN

    assert PROFILE_CLAUDE_PLUGIN == _PLUGIN
    assert PROFILE_AGENT_SKILLS_STRICT == _STRICT

    bundle = _bundle(tmp_path, '{"name": "demo"}')
    _write_spec_skill(bundle, f'name: demo\n{_D}allowed-tools: "Read Bash"\n')

    implicit = validate_bundle(bundle)
    explicit = validate_bundle(bundle, profile=PROFILE_CLAUDE_PLUGIN)
    assert implicit.valid and explicit.valid
    assert [i.code for i in implicit.warnings] == [i.code for i in explicit.warnings]


def test_a_typoed_profile_raises_rather_than_silently_falling_back(tmp_path: Path) -> None:
    """A typo must not become a false PASS on a publishability gate.

    ``agent-skills-strct`` silently resolving to the lenient profile would report
    a bundle as publishable that was never strictly checked. That is the same
    "a typo becomes permission widening" shape ``ToolPolicy`` already refuses, so
    it is a caller error (``ValueError``), not a ``ValidationIssue``.
    """
    bundle = _bundle(tmp_path, '{"name": "demo"}')
    _write_spec_skill(bundle, f"name: demo\n{_D}")

    with pytest.raises(ValueError) as exc:
        validate_bundle(bundle, profile="agent-skills-strct")
    message = str(exc.value)
    assert "agent-skills-strct" in message
    # Both valid ids must be named: the message is the only place an author
    # learns what the alternatives are.
    assert _PLUGIN in message
    assert _STRICT in message


@pytest.mark.parametrize(
    "relative",
    [
        "runner/src/curie_runner/plugin.py",
        "apps/api/src/curie_api/bundles.py",
    ],
)
def test_production_callers_never_pass_a_profile(relative: str) -> None:
    """The hard constraint made executable instead of left to review.

    Runner boot and deploy ingestion stay on the lenient default permanently. If
    either ever passed ``profile="agent-skills-strict"``, every bundle in the
    fleet carrying a block list or ``allowed-tools: []`` would stop deploying.
    Reviewers do not reliably catch a one-keyword addition; this test does.
    """
    repo_root = Path(__file__).parents[3]
    source = (repo_root / relative).read_text(encoding="utf-8")
    assert "validate_bundle(" in source, f"{relative} no longer calls validate_bundle"
    assert "profile=" not in source, f"{relative} must call validate_bundle with no profile"
