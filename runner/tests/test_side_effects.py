"""The side-effect classifier: harness-declared read-only allowlist, deny-by-default."""

from curie_runner import (
    CLAUDE_READONLY_TOOLS,
    DEFAULT_IDEMPOTENT_TOOLS,
    PLATFORM_IDEMPOTENT_TOOLS,
    SideEffectClassifier,
)


def test_read_only_tools_are_idempotent() -> None:
    classifier = SideEffectClassifier()
    for tool in DEFAULT_IDEMPOTENT_TOOLS:
        assert not classifier.is_side_effecting(tool)


def test_mutating_and_unknown_tools_flag() -> None:
    classifier = SideEffectClassifier()
    # Known mutating tools and any unknown/new tool are treated as side-effecting.
    for tool in ("Bash", "Write", "Edit", "SomeBrandNewTool"):
        assert classifier.is_side_effecting(tool)


def test_override_replaces_the_claude_declaration() -> None:
    classifier = SideEffectClassifier(["Bash"])
    assert not classifier.is_side_effecting("Bash")
    # An explicit harness declaration replaces (does not extend) the Claude set.
    assert classifier.is_side_effecting("Read")


def test_platform_approval_tool_is_idempotent_under_any_harness() -> None:
    """The platform approval tool (ADR-0010) is idempotent regardless of harness.

    It is platform-injected, not harness-shipped, so even a harness declaration
    that omits it must not be able to make the approval turn look side-effecting
    -- flagging it would block the no-retry rule for the turns approvals pause.
    """

    approval_tool = "mcp__curie__request_approval"
    assert approval_tool in PLATFORM_IDEMPOTENT_TOOLS
    # A harness declaration that omits the approval tool still classifies it safe.
    classifier = SideEffectClassifier(["read"])
    assert not classifier.is_side_effecting(approval_tool)


def test_platform_progress_tool_is_idempotent_under_any_harness() -> None:
    """A factory execution's ``report_progress`` call (#3077) never acts on the world.

    It reports a phase to the status card, so a repeat on retry duplicates nothing
    real. Flagging it disarmed the automatic retry of every factory turn that had
    reported progress, and put a changed-nothing call on the turn's receipt (#3464).
    The name comes from the constant that mounts the tool, so the two cannot drift.
    """

    from curie_runner.approval import PROGRESS_TOOL_NAME

    assert PROGRESS_TOOL_NAME in PLATFORM_IDEMPOTENT_TOOLS
    classifier = SideEffectClassifier(["read"])
    assert not classifier.is_side_effecting(PROGRESS_TOOL_NAME)


def test_the_turn_progress_tool_is_idempotent_under_any_harness() -> None:
    """ADR 0130's ``progress`` tool reports task state and acts on nothing, so a
    repeat on retry duplicates nothing real and the turn stays retryable. The
    name comes from the constant that mounts the tool, so the two cannot drift."""

    from curie_runner.approval import TURN_PROGRESS_TOOL_NAME

    assert TURN_PROGRESS_TOOL_NAME == "mcp__curie__progress"
    assert TURN_PROGRESS_TOOL_NAME in PLATFORM_IDEMPOTENT_TOOLS
    classifier = SideEffectClassifier(["read"])
    assert not classifier.is_side_effecting(TURN_PROGRESS_TOOL_NAME)


def test_opencode_named_read_only_tools_do_not_flag() -> None:
    """#308 regression: a second harness declares lowercase read-only names.

    Under a harness like OpenCode the read-only tools are ``read``/``grep``/
    ``webfetch`` (lowercase), not the Claude PascalCase identifiers. When the
    classifier is constructed with that harness's declaration, those tools must
    NOT raise side_effect_flag; an unknown tool still MUST (deny-by-default).
    """

    opencode_readonly = {"read", "grep", "glob", "list", "webfetch"}
    classifier = SideEffectClassifier(opencode_readonly)

    for tool in opencode_readonly:
        assert not classifier.is_side_effecting(tool)

    # Deny-by-default invariant holds: an unknown tool still flags. This includes
    # the Claude PascalCase names, which are not in the OpenCode declaration.
    for tool in ("bash", "write", "edit", "SomeBrandNewTool", "Read"):
        assert classifier.is_side_effecting(tool)


def test_default_is_the_claude_declaration_plus_platform_tools() -> None:
    """AC2: the historical Claude allowlist is the Claude adapter's declaration."""

    assert DEFAULT_IDEMPOTENT_TOOLS == CLAUDE_READONLY_TOOLS | PLATFORM_IDEMPOTENT_TOOLS
    # The Claude declaration is the classifier's default when none is supplied.
    default_classifier = SideEffectClassifier()
    claude_classifier = SideEffectClassifier(CLAUDE_READONLY_TOOLS)
    for tool in CLAUDE_READONLY_TOOLS:
        assert not default_classifier.is_side_effecting(tool)
        assert not claude_classifier.is_side_effecting(tool)


def test_curie_state_reads_are_idempotent_as_the_runner_builds_the_classifier() -> None:
    """A ``curie-state`` read changes nothing, so it is not a side effect.

    Classifying ``get`` and ``list`` as side effects put every state read on the
    turn's receipt as a change that "cannot be undone" and disarmed the retry of
    any run that only read state. The writes (``set``, ``append``, ``delete``)
    really do change the store and must keep flagging. The classifier here is
    built the way ``build_runner`` builds it, and the names come from the tools
    the state server actually registers, so neither can drift from the test.
    """

    from curie_runner.__main__ import _readonly_tools
    from curie_runner.harness.claude import get_contribution
    from curie_runner.state import STATE_SERVER_NAME, STATE_TOOL_NAMES

    classifier = SideEffectClassifier(
        readonly_tools=_readonly_tools(get_contribution(), frozenset(), None)
    )

    def name(tool: str) -> str:
        return f"mcp__{STATE_SERVER_NAME}__{tool}"

    reads = {name("get"), name("list")}
    writes = {name("set"), name("append"), name("delete")}
    assert reads | writes == STATE_TOOL_NAMES
    for tool in reads:
        assert not classifier.is_side_effecting(tool), tool
    for tool in writes:
        assert classifier.is_side_effecting(tool), tool

    # Platform-injected, so a harness declaration that omits them changes nothing.
    other_harness = SideEffectClassifier(["read"])
    for tool in reads:
        assert not other_harness.is_side_effecting(tool), tool
    for tool in writes:
        assert other_harness.is_side_effecting(tool), tool
