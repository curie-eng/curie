"""Safe gate-summary template grammar (#2565).

The deploy validator and the runtime renderer share this module so a template
that validates green cannot render under different rules.
"""

from plugin_format.gate_summary import (
    RESERVED_PERMISSION_PREFIX,
    check_gate_summary_template,
    render_gate_summary,
)


def test_scalar_and_count_render() -> None:
    template = (
        "File {period}: {expected|count} workbooks into Approved, "
        "{going_out_blank|count} cells going out blank. Approve?"
    )
    assert check_gate_summary_template(template) is None
    rendered = render_gate_summary(
        template,
        {
            "period": "FY26Q1",
            "expected": {"a.xlsx": "aaa", "b.xlsx": "bbb"},
            "going_out_blank": ["I35", "J12", "K1"],
        },
    )
    assert rendered == (
        "File FY26Q1: 2 workbooks into Approved, 3 cells going out blank. Approve?"
    )


def test_length_filter_on_a_string() -> None:
    assert render_gate_summary("note is {body|length} chars", {"body": "abcd"}) == (
        "note is 4 chars"
    )


def test_missing_key_returns_none() -> None:
    assert render_gate_summary("File {period}", {"other": "x"}) is None


def test_non_scalar_bare_placeholder_returns_none() -> None:
    assert render_gate_summary("dump {expected}", {"expected": {"a": "b"}}) is None


def test_count_on_a_scalar_returns_none() -> None:
    assert render_gate_summary("{n|count} items", {"n": 3}) is None


def test_unknown_filter_is_invalid() -> None:
    err = check_gate_summary_template("{files|json}")
    assert err is not None
    assert "filter" in err.lower()


def test_unmatched_brace_is_invalid() -> None:
    assert check_gate_summary_template("File {period") is not None
    assert check_gate_summary_template("File period}") is not None


def test_empty_template_is_invalid() -> None:
    assert check_gate_summary_template("   ") is not None


def test_reserved_prefix_is_invalid() -> None:
    err = check_gate_summary_template(f"{RESERVED_PERMISSION_PREFIX}not allowed")
    assert err is not None


def test_nested_ident_is_invalid() -> None:
    assert check_gate_summary_template("{foo.bar}") is not None


def test_newlines_in_scalar_collapse() -> None:
    rendered = render_gate_summary("do {title}", {"title": "one\n\ntwo"})
    assert rendered == "do one two"
    assert "\n" not in rendered


def test_slack_markup_in_scalar_is_escaped() -> None:
    rendered = render_gate_summary("do {title}", {"title": "<@U_TARGET> <!channel>"})
    assert rendered is not None
    assert "<@U_TARGET>" not in rendered
    assert "<!channel>" not in rendered
    assert "&lt;" in rendered


def test_markdown_link_in_scalar_cannot_form_a_slack_link() -> None:
    rendered = render_gate_summary(
        "Approve {title}?",
        {"title": "[Review](https://evil.example.com)"},
    )
    assert rendered is not None
    assert "[" not in rendered
    assert "]" not in rendered
    assert "Approve Review(https://evil.example.com)?" == rendered
