"""BootEnv.channel_kind is the optional worker channel kind (#3818)."""

from __future__ import annotations

from aci_protocol import PROTOCOL_VERSION, BootEnv, Budget

_SUBSTRATE_ENV = {
    "CURIE_SANDBOX_ID": "curie-sandbox-abc123",
    "CURIE_RUNNER_PORT": "8080",
}


def _env() -> dict[str, str]:
    rendered = BootEnv.render_worker(
        plugin_dir="/plugins/bundle",
        session_id="agent-sender-thread",
        budget=Budget(max_output_tokens_per_run=4096, max_usd_per_day=5.0),
        memory_ref="http://api/agents/a/state/memory",
        history_ref="http://api/agents/a/state/transcript/t",
    )
    return rendered | _SUBSTRATE_ENV


def test_channel_kind_parses_when_present_and_is_absent_as_none() -> None:
    assert PROTOCOL_VERSION == "0.5.17"
    parsed = BootEnv.from_env(_env() | {"CURIE_CHANNEL_KIND": "slack"})
    assert parsed.channel_kind == "slack"
    missing = BootEnv.from_env(_env())
    assert missing.channel_kind is None
