from pathlib import Path

import pytest
from plugin_format import validate_bundle

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


def test_valid_delegates_to_passes(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "alice", "delegatesTo": ["bob", "carol"]}')
    assert validate_bundle(bundle).valid


def test_malformed_delegates_to_shape_is_rejected(tmp_path: Path) -> None:
    # A non-list delegatesTo value is rejected (matches test_malformed_secrets_shape_is_rejected:
    # the field's own `list[str] | None` type rejects it at the manifest level before
    # `_validate_delegates_to` runs, same as `secrets`).
    bundle = _bundle(tmp_path, '{"name": "demo", "delegatesTo": "bob"}')
    assert not validate_bundle(bundle).valid


def test_empty_delegates_to_entry_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "demo", "delegatesTo": [""]}')
    assert "delegatesTo.name_invalid" in _codes(bundle)


def test_self_referential_delegates_to_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "alice", "delegatesTo": ["alice"]}')
    assert "delegatesTo.self_reference" in _codes(bundle)


def test_duplicate_delegates_to_entry_is_rejected(tmp_path: Path) -> None:
    bundle = _bundle(tmp_path, '{"name": "alice", "delegatesTo": ["bob", "bob"]}')
    assert "delegatesTo.duplicate" in _codes(bundle)


def test_valid_approval_policy_passes(tmp_path: Path) -> None:
    bundle = _bundle(
        tmp_path,
        '{"name": "demo", "approvalPolicy": {"gates": ['
        '{"gate": "PreToolUse", "route": "manager-approval"}]}}',
    )
    assert validate_bundle(bundle).valid


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
