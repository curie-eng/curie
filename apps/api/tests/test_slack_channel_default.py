"""The CLI's default Slack channel must satisfy the API's channel-ID validation.

Regression guard for #341: the CLI shipped `#local-dev` as its default channel,
which the API's own AgentCreate validator rejects with 422, so
`curie local deploy` with no --slack-channel failed on a fresh stack. This test
reads the real Rust const and runs it through the real API validator so the two
literals cannot drift back apart across the language boundary.

After ADR-0096 (#1459) the validator is kind-dispatched: one entry point,
`_validate_channel_binding(kind, address)`, chooses the address-shape rule by
kind instead of assuming Slack. The CLI's default is still a Slack binding, so
this pin follows it onto the `slack` arm rather than being dropped. Dropping it
is how #341 comes back: the CLI keeps only a fast local check for UX, and the
API is the authoritative gate, so nothing else compares the two literals.
"""

import re
from pathlib import Path

_API_RS = Path(__file__).resolve().parents[3] / "cli" / "src" / "api.rs"
_DEFAULT_RE = re.compile(r'DEFAULT_SLACK_CHANNEL:\s*&str\s*=\s*"([^"]*)"')


def _cli_default_channel() -> str:
    match = _DEFAULT_RE.search(_API_RS.read_text(encoding="utf-8"))
    assert match, f"could not find DEFAULT_SLACK_CHANNEL in {_API_RS}"
    return match.group(1)


def test_cli_default_channel_passes_api_validation() -> None:
    # Imported inside the test on purpose. The symbol does not exist until the
    # kind-dispatched validator lands, and a module-level import would turn that
    # into a COLLECTION error, taking the rest of this module's signal with it
    # and reading like a broken test file rather than an absent feature.
    from curie_api.schemas.channels import validate_channel_binding

    value = _cli_default_channel()
    # Validator raises on a bad shape and echoes the address back on success.
    assert validate_channel_binding("slack", value) == value


def test_an_unregistered_kind_does_not_borrow_the_slack_shape_rule() -> None:
    """The other half of the dispatch, and the half that makes #1459's "binds a
    non-Slack channel kind without schema changes" true.

    A kind with no registered address shape must validate on the generic rule,
    not fall through to Slack's `^[CDG][A-Z0-9]{7,}$`. If it did, every new
    adapter would need a schema change before it could bind anything, which is
    exactly the coupling ADR-0096 removes.
    """

    from curie_api.schemas.channels import validate_channel_binding

    # Rejected as a Slack address, accepted as a webhook one.
    assert validate_channel_binding("webhook", "acme-room-7") == "acme-room-7"
