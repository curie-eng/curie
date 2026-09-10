import pytest
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    ApprovalGate,
    McpServer,
    PluginManifest,
    SkillFrontmatter,
    ToolPolicy,
)
from pydantic import ValidationError


def test_manifest_requires_name() -> None:
    with pytest.raises(ValidationError):
        PluginManifest.model_validate({"description": "no name"})


def test_manifest_accepts_and_keeps_unknown_keys() -> None:
    manifest = PluginManifest.model_validate({"name": "demo", "futureField": 42})
    assert manifest.name == "demo"
    assert manifest.model_dump().get("futureField") == 42


def test_manifest_author_may_be_string_or_object() -> None:
    assert PluginManifest.model_validate({"name": "d", "author": "Jane"}).author == "Jane"
    obj = PluginManifest.model_validate({"name": "d", "author": {"name": "Jane"}})
    assert obj.author.name == "Jane"  # type: ignore[union-attr]


def test_skill_frontmatter_alias_and_required_fields() -> None:
    fm = SkillFrontmatter.model_validate(
        {"name": "greeter", "description": "greets", "allowed-tools": ["Bash"]}
    )
    assert fm.allowed_tools == ["Bash"]

    with pytest.raises(ValidationError):
        SkillFrontmatter.model_validate({"name": "greeter"})


def test_mcp_server_accepts_stdio_and_remote_shapes() -> None:
    stdio = McpServer.model_validate({"command": "python", "args": ["-m", "x"]})
    remote = McpServer.model_validate({"type": "http", "url": "https://example.com"})
    assert stdio.command == "python"
    assert remote.url == "https://example.com"


def test_manifest_system_prompt_field() -> None:
    """The Curie ``systemPrompt`` authoring extension round-trips (#271)."""
    manifest = PluginManifest.model_validate(
        {"name": "demo", "systemPrompt": "Be terse; cite the CRM record, not the message."}
    )
    assert manifest.systemPrompt == "Be terse; cite the CRM record, not the message."
    # Absent -> None (backward compatible; bundles without it still validate).
    assert PluginManifest.model_validate({"name": "demo"}).systemPrompt is None
    # Serializes back under the verbatim camelCase key.
    assert manifest.model_dump(exclude_none=True)["systemPrompt"].startswith("Be terse")


def test_manifest_starter_prompts_round_trip() -> None:
    manifest = PluginManifest.model_validate(
        {"name": "demo", "starterPrompts": ["Show open issues", "Summarize activity"]}
    )
    assert manifest.starterPrompts == ["Show open issues", "Summarize activity"]
    assert PluginManifest.model_validate({"name": "demo"}).starterPrompts is None


def test_manifest_secrets_field() -> None:
    """The Curie ``secrets`` policy extension round-trips (ADR-0009 / #429)."""
    manifest = PluginManifest.model_validate(
        {"name": "demo", "secrets": ["GITHUB_PERSONAL_ACCESS_TOKEN"]}
    )
    assert manifest.secrets == ["GITHUB_PERSONAL_ACCESS_TOKEN"]
    # Absent -> None (backward compatible; bundles without it still validate).
    assert PluginManifest.model_validate({"name": "demo"}).secrets is None
    # Serializes back under the verbatim key.
    assert manifest.model_dump(exclude_none=True)["secrets"] == ["GITHUB_PERSONAL_ACCESS_TOKEN"]


def test_manifest_trigger_and_approval_policy_fields() -> None:
    """The Curie trigger + approval-policy authoring extensions parse (#273)."""
    manifest = PluginManifest.model_validate(
        {
            "name": "demo",
            "triggers": [{"type": "cron", "schedule": "0 9 * * 1-5"}],
            "approvalPolicy": {"gates": [{"gate": "PreToolUse", "route": "manager"}]},
        }
    )
    assert manifest.triggers == [{"type": "cron", "schedule": "0 9 * * 1-5"}]
    assert manifest.approvalPolicy == {"gates": [{"gate": "PreToolUse", "route": "manager"}]}
    # Absent -> None (backward compatible).
    bare = PluginManifest.model_validate({"name": "demo"})
    assert bare.triggers is None and bare.approvalPolicy is None


def test_approval_gate_grantable_via_policy_field() -> None:
    """The operator opt-in ``grantableViaPolicy`` round-trips on ApprovalGate (#558).

    A gate the operator explicitly marks may mint a one-shot grant on a policy
    approval; absent, it defaults False (the #544 no-grant baseline), so an old
    manifest keeps its behavior.
    """

    gate = ApprovalGate.model_validate(
        {"gate": "close_issue", "route": "deal-desk", "grantableViaPolicy": True}
    )
    assert gate.grantableViaPolicy is True
    # Absent -> False (backward compatible; existing gates keep the no-grant default).
    gate2 = ApprovalGate.model_validate({"gate": "close_issue", "route": "deal-desk"})
    assert gate2.grantableViaPolicy is False
    # Serializes back under the verbatim camelCase key and round-trips.
    dumped = gate.model_dump()
    assert dumped["grantableViaPolicy"] is True
    assert ApprovalGate.model_validate(dumped).grantableViaPolicy is True


def test_approval_gate_summary_template_field() -> None:
    """A bundle-authored human template round-trips on ApprovalGate (#2565).

    Absent -> None so an old manifest keeps today's machine-string card.
    """

    gate = ApprovalGate.model_validate(
        {
            "gate": "Bash",
            "route": "managers",
            "summary": "Run {command}: {files|count} files. Approve?",
        }
    )
    assert gate.summary == "Run {command}: {files|count} files. Approve?"
    gate2 = ApprovalGate.model_validate({"gate": "Bash", "route": "managers"})
    assert gate2.summary is None
    dumped = gate.model_dump()
    assert dumped["summary"] == "Run {command}: {files|count} files. Approve?"
    assert ApprovalGate.model_validate(dumped).summary == gate.summary


def test_tool_policy_parses_a_full_declaration_and_defaults_its_collections() -> None:
    """The ``toolPolicy`` model round-trips, and its three collections default to empty.

    Empty is the coherent default for a DECLARED policy: unmatched means deny, so a
    policy that lists nothing refuses everything. The alternative -- ``None`` for an
    omitted collection -- would invite a "None means unrestricted" reading, which is
    the fail-open the policy exists to prevent.
    """

    policy = ToolPolicy.model_validate(
        {
            "enforcement": TOOL_POLICY_ENFORCEMENT,
            "allow": ["grafana/list_datasources"],
            "approvalRequired": ["kubernetes/pods_*"],
            "deny": ["kubernetes/resources_delete"],
        }
    )
    assert policy.enforcement == TOOL_POLICY_ENFORCEMENT
    assert policy.allow == ["grafana/list_datasources"]
    assert policy.approvalRequired == ["kubernetes/pods_*"]
    assert policy.deny == ["kubernetes/resources_delete"]
    # Serializes back under the verbatim camelCase key.
    assert policy.model_dump()["approvalRequired"] == ["kubernetes/pods_*"]

    bare = ToolPolicy.model_validate({"enforcement": TOOL_POLICY_ENFORCEMENT})
    assert bare.allow == []
    assert bare.approvalRequired == []
    assert bare.deny == []


def test_tool_policy_rejects_an_unknown_key() -> None:
    """ToolPolicy is STRICT: an unknown key is a typo, and a typo here widens permissions.

    The concrete failure a lenient model would ship: an author writes ``denny``
    for ``deny``, pydantic accepts and discards it, the real deny list is empty,
    and every tool the author believed blocked falls through to their ``allow``
    glob. Forward-compatibility leniency is right for shapes with an EXTERNAL
    producer (PluginManifest mirrors Claude Code's evolving format); a
    Curie-owned policy object has no such producer, so ``extra="forbid"`` --
    the same reasoning ConnectorSpec, ConnectorLockEntry and DeployTarget encode.
    """

    with pytest.raises(ValidationError):
        ToolPolicy.model_validate({"enforcement": TOOL_POLICY_ENFORCEMENT, "futureField": 42})

    # The realistic case, stated as itself: a misspelled collection name.
    with pytest.raises(ValidationError):
        ToolPolicy.model_validate(
            {
                "enforcement": TOOL_POLICY_ENFORCEMENT,
                "allow": ["k8s/*"],
                "denny": ["k8s/delete_namespace"],
            }
        )


def test_manifest_stays_lenient_even_though_tool_policy_is_strict() -> None:
    """Strictness is scoped to ToolPolicy; PluginManifest keeps accepting unknown keys.

    Tightening the manifest would reject real Claude Code bundles carrying fields
    this package does not model, which is the compatibility wedge.
    """

    manifest = PluginManifest.model_validate(
        {
            "name": "demo",
            "futureField": 42,
            "toolPolicy": {"enforcement": TOOL_POLICY_ENFORCEMENT, "allow": ["grafana/*"]},
        }
    )
    assert manifest.model_dump().get("futureField") == 42
    assert manifest.toolPolicy is not None


def test_manifest_tool_policy_field() -> None:
    """The Curie ``toolPolicy`` authoring extension round-trips on the manifest.

    Absent -> ``None``, which is the backward-compatibility guarantee: every bundle
    shipped before this feature keeps validating and keeps its behavior. The one
    visible change for an existing bundle is that ``model_dump()`` now carries
    ``toolPolicy: None`` -- the ordinary new-optional-field patch behaviour, which
    is why the round-trip below dumps with ``exclude_none``.
    """

    declared = {
        "enforcement": TOOL_POLICY_ENFORCEMENT,
        "allow": ["grafana/*"],
        "deny": ["grafana/delete_*"],
    }
    manifest = PluginManifest.model_validate({"name": "demo", "toolPolicy": declared})
    assert manifest.toolPolicy == declared
    assert manifest.model_dump(exclude_none=True)["toolPolicy"] == declared

    assert PluginManifest.model_validate({"name": "demo"}).toolPolicy is None


def test_skill_frontmatter_accepts_the_canonical_string_form_verbatim() -> None:
    """``allowed-tools`` accepts the space-separated string the spec calls canonical.

    The model widening is the headline fix: a SKILL.md written for the Agent
    Skills specification is rejected today with "Input should be a valid list".
    Restoring it is a return to this model's ORIGINAL intent -- mirroring the
    Claude Code shape verbatim -- not a departure from it.
    """
    fm = SkillFrontmatter.model_validate(
        {"name": "greeter", "description": "greets", "allowed-tools": "Read Bash"}
    )
    assert fm.allowed_tools == "Read Bash"

    # The comma-separated string Claude Code equally documents.
    comma = SkillFrontmatter.model_validate(
        {"name": "greeter", "description": "greets", "allowed-tools": "Read,Bash"}
    )
    assert comma.allowed_tools == "Read,Bash"

    # Absent stays None, so every bundle shipped before this widening is untouched.
    assert (
        SkillFrontmatter.model_validate({"name": "greeter", "description": "d"}).allowed_tools
        is None
    )


def test_the_model_preserves_the_authored_shape_and_does_not_normalize() -> None:
    """One normalization boundary, and the model is not it (D1).

    If the model normalized, there would be two places that know how to read the
    field, and ``runner/src/curie_runner/approval.py`` -- which parses the raw
    YAML itself and never builds a ``SkillFrontmatter`` -- would be the one left
    behind. That divergence is the #1852 fail-open: a gate reported as armed
    while the tool executes unapproved.
    """
    from plugin_format import parse_allowed_tools

    fm = SkillFrontmatter.model_validate(
        {"name": "greeter", "description": "greets", "allowed-tools": "Read Bash"}
    )
    # The model hands back exactly what the author wrote...
    assert isinstance(fm.allowed_tools, str)
    assert fm.allowed_tools == "Read Bash"
    # ...and the helper is the only thing that turns it into entries.
    assert parse_allowed_tools(fm.allowed_tools) == ["Read", "Bash"]

    # The list form is likewise preserved, not rewritten into a string.
    listed = SkillFrontmatter.model_validate(
        {"name": "greeter", "description": "greets", "allowed-tools": ["Read", "Bash"]}
    )
    assert listed.allowed_tools == ["Read", "Bash"]
    assert parse_allowed_tools(listed.allowed_tools) == ["Read", "Bash"]

    # Round-tripping through the alias keeps the authored shape.
    assert fm.model_dump(by_alias=True)["allowed-tools"] == "Read Bash"


def test_the_committed_schema_declares_the_string_form() -> None:
    """Intent-bearing cover for the regenerated schema, not just a byte diff.

    ``test_schema_compat.py`` proves the committed file matches what pydantic
    renders TODAY. It cannot tell you that a future regeneration silently
    narrowed ``allowed-tools`` back to a list -- both sides would move together
    and the gate would stay green. This asserts the meaning.
    """
    import json
    from pathlib import Path

    schema_path = Path(__file__).parents[1] / "schema" / "plugin-format.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    any_of = schema["$defs"]["SkillFrontmatter"]["properties"]["allowed-tools"]["anyOf"]

    assert {"type": "string"} in any_of, any_of
    # The widening is purely additive: the list and null members must survive, or
    # every bundle carrying a block list stops validating against the schema.
    assert {"type": "array", "items": {"type": "string"}} in any_of, any_of
    assert {"type": "null"} in any_of, any_of
