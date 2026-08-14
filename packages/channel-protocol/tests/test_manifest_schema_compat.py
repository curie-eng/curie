from channel_protocol.manifest_schema import render_schema, schema_path


def test_committed_manifest_schema_is_current() -> None:
    assert schema_path().read_text(encoding="utf-8") == render_schema(), (
        "adapter profile schema is stale; run python -m channel_protocol.manifest_schema"
    )
