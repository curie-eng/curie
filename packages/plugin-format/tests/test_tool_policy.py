"""Unit table for ``plugin_format.tool_policy``: the vanilla MCP tool policy.

The policy answers ONE question for the runtime: given a canonical tool name
``"<server>/<tool>"``, may the agent call it, must a human approve it, or is it
refused outright? Every case here pins a decision that fails CLOSED, because the
failure mode this module exists to prevent is a policy that reads as protective
and silently classifies nothing (the #453/#544 fail-open shape).

The canonical name is deliberately NOT the SDK's live tool name
(``mcp__plugin_<bundle>_<server>__<tool>``). It is server-qualified and
transport-independent, so a bundle author writes one policy that survives a
server moving between the plugin ``mcpServers`` map and ``connectors.yaml``.
"""

import fnmatch

import plugin_format.tool_policy
import pytest
from plugin_format import (
    TOOL_POLICY_ENFORCEMENT,
    PluginManifest,
    ToolPolicy,
    ToolPolicyDecision,
    ToolPolicyInvalid,
    ToolPolicyUnenforceable,
    classify_tool,
    load_tool_policy,
    validate_pattern,
)


def _policy(
    *,
    allow: list[str] | None = None,
    approval_required: list[str] | None = None,
    deny: list[str] | None = None,
) -> ToolPolicy:
    """A ToolPolicy carrying the supported enforcement id and the given collections."""

    return ToolPolicy.model_validate(
        {
            "enforcement": TOOL_POLICY_ENFORCEMENT,
            "allow": allow or [],
            "approvalRequired": approval_required or [],
            "deny": deny or [],
        }
    )


# --- the contract identifiers -------------------------------------------------


def test_enforcement_id_is_the_versioned_contract_string() -> None:
    """The id is a wire constant: a build that implements v1 advertises exactly this.

    Pinned as a literal because a bundle declares it verbatim in its manifest and
    the loader refuses anything else; changing it is a contract break, not a
    rename.
    """

    assert TOOL_POLICY_ENFORCEMENT == "curie/mcp-tool-policy@1"


def test_decision_values_are_the_wire_strings() -> None:
    """The decision is serialized into approval records and logs, so its values are pinned."""

    assert ToolPolicyDecision.ALLOW == "allow"
    assert ToolPolicyDecision.APPROVAL_REQUIRED == "approval-required"
    assert ToolPolicyDecision.DENY == "deny"


# --- classification: precedence, fail-closed default, matching rules -----------


def test_precedence_is_by_class_never_by_specificity() -> None:
    """deny > approval-required > allow, even when the LOSING pattern is more specific.

    "Most specific wins" is the tempting wrong reading, and it is the dangerous
    one: here the narrow ``allow`` of ``kubernetes/resources_scale`` sits inside
    the broad ``approvalRequired`` glob ``kubernetes/resources_*``. Specificity
    would silently drop the human out of a scaling operation. Class precedence
    keeps the approval.
    """

    policy = _policy(
        allow=["kubernetes/resources_scale"],
        approval_required=["kubernetes/resources_*"],
        deny=["kubernetes/resources_create_or_update"],
    )

    # deny wins over the broader approvalRequired glob that also matches it.
    assert classify_tool(policy, "kubernetes/resources_create_or_update") == ToolPolicyDecision.DENY
    # The broader approval glob outranks the NARROWER allow: class, not specificity.
    assert (
        classify_tool(policy, "kubernetes/resources_scale") == ToolPolicyDecision.APPROVAL_REQUIRED
    )
    # And a name only the approval glob matches is unremarkable.
    assert (
        classify_tool(policy, "kubernetes/resources_delete") == ToolPolicyDecision.APPROVAL_REQUIRED
    )


def test_unmatched_tool_is_denied() -> None:
    """A name no collection mentions is DENIED, not allowed.

    The whole point of the policy: the default is refusal, so a policy can only
    ever widen the surface deliberately.
    """

    policy = _policy(allow=["grafana/list_datasources"])

    assert classify_tool(policy, "grafana/list_datasources") == ToolPolicyDecision.ALLOW
    assert classify_tool(policy, "grafana/delete_datasource") == ToolPolicyDecision.DENY


@pytest.mark.parametrize(
    ("collections", "expected"),
    [
        ({"allow": ["curie-slack/history"]}, ToolPolicyDecision.ALLOW),
        ({"approvalRequired": ["curie-slack/history"]}, ToolPolicyDecision.APPROVAL_REQUIRED),
        ({"deny": ["curie-slack/history"]}, ToolPolicyDecision.DENY),
        (
            {"allow": ["curie-slack/history"], "approvalRequired": ["curie-slack/*"]},
            ToolPolicyDecision.APPROVAL_REQUIRED,
        ),
        (
            {
                "allow": ["curie-slack/hist*"],
                "approvalRequired": ["curie-slack/*"],
                "deny": ["curie-slack/history"],
            },
            ToolPolicyDecision.DENY,
        ),
        ({"allow": ["curie-slack/replies"]}, ToolPolicyDecision.DENY),
    ],
)
def test_channel_read_grant_does_not_override_tool_policy(
    collections: dict[str, list[str]], expected: ToolPolicyDecision
) -> None:
    manifest = PluginManifest.model_validate(
        {
            "name": "acme-bot",
            "channelRead": True,
            "toolPolicy": {"enforcement": TOOL_POLICY_ENFORCEMENT, **collections},
        }
    )
    assert manifest.channelRead is True
    policy = load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)
    assert policy is not None
    assert classify_tool(policy, "curie-slack/history") is expected


@pytest.mark.parametrize(
    ("grant", "tool"),
    [
        ("canvasList", "curie-slack/list_channel_canvases"),
        ("canvasRead", "curie-slack/read_canvas"),
        ("canvasEdit", "curie-slack/edit_canvas_cell"),
    ],
)
@pytest.mark.parametrize(
    ("collection", "expected"),
    [
        ("allow", ToolPolicyDecision.ALLOW),
        ("approvalRequired", ToolPolicyDecision.APPROVAL_REQUIRED),
        ("deny", ToolPolicyDecision.DENY),
    ],
)
def test_canvas_grant_does_not_override_tool_policy(
    grant: str, tool: str, collection: str, expected: ToolPolicyDecision
) -> None:
    manifest = PluginManifest.model_validate(
        {
            "name": "acme-bot",
            grant: True,
            "toolPolicy": {"enforcement": TOOL_POLICY_ENFORCEMENT, collection: [tool]},
        }
    )
    assert getattr(manifest, grant) is True
    policy = load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)
    assert policy is not None
    assert classify_tool(policy, tool) is expected


@pytest.mark.parametrize("grant", ["canvasList", "canvasRead", "canvasEdit"])
def test_canvas_grant_does_not_allow_a_tool_the_policy_never_mentions(grant: str) -> None:
    manifest = PluginManifest.model_validate(
        {
            "name": "acme-bot",
            grant: True,
            "toolPolicy": {
                "enforcement": TOOL_POLICY_ENFORCEMENT,
                "allow": ["curie-slack/history"],
            },
        }
    )
    policy = load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)
    assert policy is not None
    assert classify_tool(policy, "curie-slack/edit_canvas_cell") is ToolPolicyDecision.DENY


def test_a_newly_advertised_tool_the_policy_never_mentions_is_denied() -> None:
    """Server drift fails CLOSED: a sixth tool appearing after the policy was written is denied.

    The real case from the tracer -- a policy written against a Grafana server's
    five published tools, then the server starts advertising ``update_dashboard``
    on its next release. Nothing in the bundle changed, so nothing re-reviews the
    policy; the new write-capable tool must not become callable by default.

    The already-classified tool is asserted in the SAME test on purpose: it proves
    drift-fails-closed rather than everything-denies, which a broken matcher would
    also satisfy.
    """

    policy = _policy(
        allow=[
            "grafana/list_datasources",
            "grafana/query_prometheus",
            "grafana/list_dashboards",
        ],
        approval_required=["grafana/create_annotation", "grafana/delete_annotation"],
    )

    # The sixth tool, advertised after the policy was authored.
    assert classify_tool(policy, "grafana/update_dashboard") == ToolPolicyDecision.DENY
    # ... while the originally-classified tools still resolve exactly as before.
    assert classify_tool(policy, "grafana/list_datasources") == ToolPolicyDecision.ALLOW
    assert (
        classify_tool(policy, "grafana/create_annotation") == ToolPolicyDecision.APPROVAL_REQUIRED
    )


def test_a_policy_with_three_empty_collections_denies_every_tool() -> None:
    """An empty declared policy is coherent, not vacuous: it refuses everything.

    "Declared but empty" must never degrade into "no policy". That degradation is
    exactly how a fail-closed control becomes a fail-open one.
    """

    policy = _policy()

    assert classify_tool(policy, "grafana/list_datasources") == ToolPolicyDecision.DENY
    assert classify_tool(policy, "kubernetes/pods_run") == ToolPolicyDecision.DENY


def test_wildcards_do_not_cross_the_server_separator() -> None:
    """``grafana/*`` is scoped to the grafana server and can never reach another one.

    A naive ``fnmatch`` over the whole string would let ``grafana/*`` swallow
    nothing extra here, but ``*/pods_run``-style patterns and any pattern ending in
    ``*`` would leak across ``/``. Server and tool segments match independently.
    """

    policy = _policy(allow=["grafana/*"])

    assert classify_tool(policy, "grafana/list_datasources") == ToolPolicyDecision.ALLOW
    assert classify_tool(policy, "kubernetes/pods_run") == ToolPolicyDecision.DENY


def test_a_wildcard_server_segment_does_not_swallow_extra_segments() -> None:
    """``*/x`` matches one server segment only -- ``a/b/x`` is a different shape and is denied."""

    policy = _policy(allow=["*/x"])

    assert classify_tool(policy, "a/x") == ToolPolicyDecision.ALLOW
    assert classify_tool(policy, "a/b/x") == ToolPolicyDecision.DENY


def test_matching_is_case_sensitive() -> None:
    """Tool names are case-sensitive on the wire, so the matcher is too.

    A case-INSENSITIVE match would widen every allow pattern beyond what the author
    wrote; ``fnmatch.fnmatchcase`` is the required primitive, not ``fnmatch``
    (which is platform-case-folding).
    """

    policy = _policy(allow=["grafana/List_*"])

    assert classify_tool(policy, "grafana/list_datasources") == ToolPolicyDecision.DENY
    assert classify_tool(policy, "grafana/List_datasources") == ToolPolicyDecision.ALLOW


def test_multi_server_policy_classifies_each_server_independently() -> None:
    """One policy spanning two servers keeps their surfaces separate, and denies a third."""

    policy = _policy(
        allow=["grafana/*"],
        approval_required=["kubernetes/pods_*"],
    )

    assert classify_tool(policy, "grafana/query_prometheus") == ToolPolicyDecision.ALLOW
    assert classify_tool(policy, "kubernetes/pods_run") == ToolPolicyDecision.APPROVAL_REQUIRED
    # The grafana allow does not reach kubernetes' non-pod tools ...
    assert classify_tool(policy, "kubernetes/resources_delete") == ToolPolicyDecision.DENY
    # ... and a third server nobody wrote a pattern for is refused entirely.
    assert classify_tool(policy, "github/create_pull_request") == ToolPolicyDecision.DENY


# --- pattern grammar ----------------------------------------------------------

_VALID_PATTERNS = [
    "grafana/list_datasources",
    "grafana/*",
    "*/pods_*",
    "*/*",
    "k8s-prod/resources_?cale",
    "my.server/tool.name",
    "a_b/c-d",
]


@pytest.mark.parametrize("pattern", _VALID_PATTERNS)
def test_valid_pattern_reports_no_reason(pattern: str) -> None:
    """A well-formed pattern returns ``None`` -- the "no reason to reject" signal."""

    assert validate_pattern(pattern) is None


_INVALID_PATTERNS = [
    pytest.param("", id="empty"),
    pytest.param("   ", id="whitespace-only"),
    pytest.param("grafana /tool", id="space-in-server"),
    pytest.param("grafana/list tool", id="space-in-tool"),
    pytest.param("grafana/list\ttool", id="tab-in-tool"),
    pytest.param("list_datasources", id="no-separator"),
    pytest.param("a/b/c", id="two-separators"),
    pytest.param("/tool", id="empty-server-segment"),
    pytest.param("server/", id="empty-tool-segment"),
    pytest.param("grafana/**", id="double-star"),
    pytest.param("**/tool", id="double-star-server"),
    pytest.param("grafana/[abc]*", id="character-class"),
    pytest.param("grafana/tool!", id="disallowed-punctuation"),
    pytest.param("graf@na/tool", id="disallowed-at-sign"),
]


@pytest.mark.parametrize("pattern", _INVALID_PATTERNS)
def test_invalid_pattern_reports_a_reason(pattern: str) -> None:
    """Every malformed pattern returns a non-empty human-readable reason.

    A silent ``None`` here would let a typo'd pattern through as an inert rule:
    the author believes a tool is allowed (or denied) and it is not. The reason
    string is what the deploy-time validator surfaces, so it must be non-empty,
    not just non-``None``.
    """

    reason = validate_pattern(pattern)
    assert reason is not None, f"{pattern!r} should have been rejected"
    assert reason.strip(), f"{pattern!r} was rejected with an empty reason"


# --- load_tool_policy: enforcement handshake ----------------------------------


@pytest.mark.parametrize(
    "enforces",
    [None, TOOL_POLICY_ENFORCEMENT, "curie/mcp-tool-policy@2", "something/else@1"],
)
def test_manifest_without_a_tool_policy_loads_none_for_every_enforces_value(
    enforces: str | None,
) -> None:
    """BACKWARD COMPATIBILITY: no ``toolPolicy`` means no policy, whatever the caller enforces.

    Every bundle that exists today has no ``toolPolicy``. If an unsupported
    ``enforces`` raised on those, shipping this module would break every deployed
    agent at once. Absence is not a declaration, so it can never be unenforceable.
    """

    manifest = PluginManifest.model_validate({"name": "demo"})

    assert load_tool_policy(manifest, enforces=enforces) is None


def test_declared_policy_loads_with_its_collections_intact() -> None:
    """The supported handshake returns a parsed policy carrying exactly what was declared."""

    manifest = PluginManifest.model_validate(
        {
            "name": "demo",
            "toolPolicy": {
                "enforcement": TOOL_POLICY_ENFORCEMENT,
                "allow": ["grafana/list_datasources", "grafana/query_*"],
                "approvalRequired": ["kubernetes/pods_*"],
                "deny": ["kubernetes/resources_delete"],
            },
        }
    )

    policy = load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)

    assert policy is not None
    assert policy.allow == ["grafana/list_datasources", "grafana/query_*"]
    assert policy.approvalRequired == ["kubernetes/pods_*"]
    assert policy.deny == ["kubernetes/resources_delete"]
    # And the loaded policy actually classifies against what it carried.
    assert classify_tool(policy, "grafana/query_prometheus") == ToolPolicyDecision.ALLOW


def test_declared_policy_is_unenforceable_when_the_caller_enforces_nothing() -> None:
    """A call site that cannot enforce the policy must REFUSE the bundle, not ignore it.

    ``enforces=None`` is the honest statement "this code path does not implement
    tool policy". Returning the policy anyway would let a caller receive it and
    quietly not apply it -- the fail-open shape. Returning ``None`` would be worse
    still: indistinguishable from "no policy declared".
    """

    manifest = PluginManifest.model_validate(
        {
            "name": "demo",
            "toolPolicy": {"enforcement": TOOL_POLICY_ENFORCEMENT, "allow": ["grafana/*"]},
        }
    )

    with pytest.raises(ToolPolicyUnenforceable):
        load_tool_policy(manifest, enforces=None)


@pytest.mark.parametrize("enforces", ["curie/mcp-tool-policy@2", "curie/mcp-tool-policy", "v1"])
def test_declared_policy_is_unenforceable_under_a_different_contract_id(enforces: str) -> None:
    """A caller implementing a DIFFERENT contract version cannot enforce this bundle's policy."""

    manifest = PluginManifest.model_validate(
        {
            "name": "demo",
            "toolPolicy": {"enforcement": TOOL_POLICY_ENFORCEMENT, "deny": ["grafana/*"]},
        }
    )

    with pytest.raises(ToolPolicyUnenforceable):
        load_tool_policy(manifest, enforces=enforces)


@pytest.mark.parametrize(
    "declared",
    ["curie/mcp-tool-policy@2", "", "curie/other-policy@1"],
)
def test_a_bundle_declaring_semantics_this_build_does_not_implement_is_unenforceable(
    declared: str,
) -> None:
    """The bundle's own ``enforcement`` value is checked too, not just the caller's.

    A bundle asking for ``@2`` semantics is asking for rules this build does not
    implement. Applying ``@1`` rules to it would silently give the author a
    different policy than the one they wrote, so it is refused even when the caller
    passes the supported id.
    """

    manifest = PluginManifest.model_validate(
        {
            "name": "demo",
            "toolPolicy": {"enforcement": declared, "allow": ["grafana/*"]},
        }
    )

    with pytest.raises(ToolPolicyUnenforceable):
        load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)


# --- the enforcement id is compared BYTE-FOR-BYTE -----------------------------
#
# The id is a wire constant, not an identifier, so no normalization is applied to
# either side of the handshake. Exact comparison is the fail-CLOSED direction: a
# stray space makes the bundle refused loudly rather than silently treated as v1.
# This is a deliberate DIFFERENCE from ``approval_policy.py``, which strips gate
# names so the validator and the runtime loader agree on one tool NAME -- a
# version discriminator is not a tool name.

_PADDED_ENFORCEMENT_IDS = [
    pytest.param(" curie/mcp-tool-policy@1", id="leading-space"),
    pytest.param("curie/mcp-tool-policy@1 ", id="trailing-space"),
    pytest.param("curie/mcp-tool-policy\t@1", id="internal-tab"),
]


@pytest.mark.parametrize("declared", _PADDED_ENFORCEMENT_IDS)
def test_a_whitespace_padded_bundle_enforcement_id_is_not_the_v1_contract(declared: str) -> None:
    """`` curie/mcp-tool-policy@1 `` is a different string, so the bundle is refused.

    Stripping it would accept a manifest that does not carry the v1 id and apply
    v1 rules to it -- silently deciding, on the author's behalf, that the padding
    was meaningless. Refusing is recoverable in one edit; a wrong-but-accepted
    contract id is invisible.
    """

    manifest = PluginManifest.model_validate(
        {
            "name": "demo",
            "toolPolicy": {"enforcement": declared, "allow": ["grafana/list_datasources"]},
        }
    )

    with pytest.raises(ToolPolicyUnenforceable):
        load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)


@pytest.mark.parametrize("enforces", _PADDED_ENFORCEMENT_IDS)
def test_a_whitespace_padded_caller_id_does_not_satisfy_the_handshake(enforces: str) -> None:
    """The CALLER's ``enforces`` is compared exactly too, not just the bundle's.

    Both sides of a two-sided handshake must normalize identically, and the only
    normalization that cannot drift is none at all.
    """

    manifest = PluginManifest.model_validate(
        {
            "name": "demo",
            "toolPolicy": {"enforcement": TOOL_POLICY_ENFORCEMENT, "allow": ["grafana/*"]},
        }
    )

    with pytest.raises(ToolPolicyUnenforceable):
        load_tool_policy(manifest, enforces=enforces)


# --- load_tool_policy applies the deploy validator's pattern rules -------------
#
# ``_matches`` hands whatever strings the collections carry straight to
# ``fnmatchcase``, so a pattern the grammar forbids still MATCHES at runtime. A
# loader that skipped ``check_policy_patterns`` would therefore hand a caller a
# policy the deploy validator refuses -- the #453/#544 validator-vs-runtime drift,
# in the direction that GRANTS tool access. Each case below names the fail-open it
# prevents.


def _policy_manifest(**collections: list[str]) -> PluginManifest:
    """A manifest carrying a v1-enforced toolPolicy with the given collections."""

    return PluginManifest.model_validate(
        {
            "name": "demo",
            "toolPolicy": {"enforcement": TOOL_POLICY_ENFORCEMENT, **collections},
        }
    )


def test_load_refuses_a_character_class_pattern() -> None:
    """FAIL-OPEN PREVENTED: ``grafana/[a-z]*`` would become a live, locale-sensitive ALLOW.

    ``validate_pattern`` rejects character classes outright, so this bundle
    cannot be deployed. Without the loader running the same check, a runtime
    following the advertised ``load_tool_policy`` -> ``classify_tool`` API would
    give the class real ``fnmatchcase`` semantics and allow tools on a bundle the
    validator had refused -- and the set of tools it allows would depend on the
    machine's locale.
    """

    manifest = _policy_manifest(allow=["grafana/[a-z]*"])

    with pytest.raises(ToolPolicyInvalid) as excinfo:
        load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)

    # The message must name the offending pattern, or the fix is a hunt.
    assert "grafana/[a-z]*" in str(excinfo.value)
    assert "character classes" in str(excinfo.value)


def test_load_refuses_a_double_star_pattern() -> None:
    """FAIL-OPEN PREVENTED: ``**`` is meaningless here but ``fnmatchcase`` still matches on it.

    A two-segment namespace has no recursive level for ``**``, so the author's
    intent is unknowable; ``fnmatchcase`` would nonetheless treat it as a plain
    wildcard and widen the segment. The deploy validator rejects it, so the
    loader must too.
    """

    manifest = _policy_manifest(allow=["grafana/**"])

    with pytest.raises(ToolPolicyInvalid) as excinfo:
        load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)

    assert "grafana/**" in str(excinfo.value)


def test_load_refuses_a_duplicate_within_one_collection() -> None:
    """The loader enforces the DUPLICATE rule too, not only the grammar.

    ``check_policy_patterns`` owns three rule families; wiring only the grammar
    into the loader would leave the other two enforced at deploy and absent at
    runtime, which is the same drift in a quieter form.
    """

    manifest = _policy_manifest(allow=["grafana/query_*", "grafana/query_*"])

    with pytest.raises(ToolPolicyInvalid) as excinfo:
        load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)

    assert "grafana/query_*" in str(excinfo.value)
    assert "more than once" in str(excinfo.value)


def test_load_refuses_the_same_pattern_declared_in_two_collections() -> None:
    """FAIL-OPEN PREVENTED: the stated intent is silently reduced to one class.

    ``kubernetes/pods_delete`` in both ``approvalRequired`` and ``allow`` reads as
    "gated AND allowed"; precedence would quietly keep only the approval and drop
    the rest of what the author wrote. The deploy validator calls that a
    declaration error, and a runtime that accepted it would apply a policy nobody
    wrote.
    """

    manifest = _policy_manifest(
        approvalRequired=["kubernetes/pods_delete"],
        allow=["kubernetes/pods_delete"],
    )

    with pytest.raises(ToolPolicyInvalid) as excinfo:
        load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)

    assert "kubernetes/pods_delete" in str(excinfo.value)


@pytest.mark.parametrize("pattern", _INVALID_PATTERNS)
def test_every_pattern_validate_pattern_rejects_also_makes_the_loader_refuse(
    pattern: str,
) -> None:
    """THE INVARIANT: the deploy validator and the loader accept exactly the same patterns.

    Stated directly rather than case by case, and parametrized over the whole
    invalid table so a rule added to ``validate_pattern`` but never wired into
    ``load_tool_policy`` fails HERE instead of shipping as a runtime hole. A
    caller must not be able to obtain a ``ToolPolicy`` carrying a pattern
    ``curie build`` would have refused.
    """

    assert validate_pattern(pattern) is not None, "table drift: this pattern is now valid"

    manifest = _policy_manifest(allow=[pattern])

    with pytest.raises(ToolPolicyInvalid):
        load_tool_policy(manifest, enforces=TOOL_POLICY_ENFORCEMENT)


def test_an_unenforceable_bundle_fails_the_handshake_before_its_patterns_are_judged() -> None:
    """ORDERING: a caller that cannot enforce is turned away whatever the policy's shape.

    The malformed pattern here is real, but the caller enforces nothing, so the
    refusal must still be ``ToolPolicyUnenforceable``. Reporting the glob instead
    would imply that fixing the glob makes this call path able to enforce the
    policy, which it never will.
    """

    manifest = _policy_manifest(allow=["grafana/[a-z]*"])

    with pytest.raises(ToolPolicyUnenforceable):
        load_tool_policy(manifest, enforces=None)


def test_the_two_refusals_are_distinguishable_exception_types() -> None:
    """Cannot-enforce and malformed are different failures with different fixes.

    A caller may reasonably tolerate one and not the other (a build with no
    runtime lane expects the first), so overloading a single exception would make
    them indistinguishable at the catch site.
    """

    assert not issubclass(ToolPolicyInvalid, ToolPolicyUnenforceable)
    assert not issubclass(ToolPolicyUnenforceable, ToolPolicyInvalid)


# --- structural guard on the matching primitive -------------------------------


def test_the_matcher_binds_fnmatchcase_and_not_fnmatch() -> None:
    """STRUCTURAL: the module must bind ``fnmatch.fnmatchcase``, checked by identity.

    ``test_matching_is_case_sensitive`` above cannot catch a swap to plain
    ``fnmatch``: POSIX ``fnmatch`` is case-SENSITIVE on Linux, the only platform
    this suite runs on, so the behavioural test passes either way here while
    every allow-pattern silently becomes case-INSENSITIVE on a case-folding
    platform (macOS, Windows) -- a platform-dependent permission WIDENING that
    the test table cannot see. This guard supplements that test; it does not
    replace it.
    """

    assert plugin_format.tool_policy.fnmatchcase is fnmatch.fnmatchcase
