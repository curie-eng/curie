"""The declared wire inventory rejects unreviewed producer and keyspace sites."""

from __future__ import annotations

from pathlib import Path

import pytest
from curie_internal.wire_inventory import (
    scan_key_literals,
    scan_producers,
    validate_inventory,
)

ROOT = Path(__file__).resolve().parents[1]


def _write(root: Path, relative: str, source: str) -> None:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(source, encoding="utf-8")


def test_all_existing_wire_sites_are_declared() -> None:
    assert validate_inventory(ROOT) == []
    producers = scan_producers(ROOT)
    literals = scan_key_literals(ROOT)
    assert any("cli/src/queue.rs" in site for site in producers)
    assert any("capacity_wait" in site for site in producers)
    # These ownership protected sites remain declared without migration.
    assert any("curie_worker/kernel/" in site for site in literals)
    assert any("curie_worker/markers.py" in site for site in literals)


@pytest.mark.parametrize(
    ("relative", "source"),
    [
        (
            "apps/api/src/curie_api/new_producer.py",
            'async def publish(client, stream, payload):\n'
            '    await client.xadd(stream, {"payload": payload})\n',
        ),
        (
            "apps/worker/src/curie_worker/new_producer.py",
            'SCRIPT = """return redis.call("XADD", KEYS[1], "*", "payload", ARGV[1])"""\n',
        ),
        (
            "cli/src/new_producer.rs",
            'fn publish() { redis::cmd("XADD").arg(stream).arg("*"); }\n',
        ),
    ],
    ids=["python", "lua", "rust"],
)
def test_undeclared_producer_is_rejected(tmp_path: Path, relative: str, source: str) -> None:
    _write(tmp_path, relative, source)
    assert any(relative in site for site in scan_producers(tmp_path))
    assert any(relative in error for error in validate_inventory(tmp_path))


@pytest.mark.parametrize(
    "source",
    [
        'KEY = "curie:undeclared:key"\n',
        "KEY = 'curie:undeclared:key'\n",
        'def key(identity):\n    return f"curie:undeclared:{identity}"\n',
        # Existing wire values do not authorize a new construction site.
        'KEY = "curie:runs"\n',
    ],
    ids=["double_quotes", "single_quotes", "formatted", "existing_value_new_site"],
)
def test_undeclared_key_literal_is_rejected(tmp_path: Path, source: str) -> None:
    relative = "apps/api/src/curie_api/new_key.py"
    _write(tmp_path, relative, source)
    assert any(relative in site for site in scan_key_literals(tmp_path))
    assert any(relative in error for error in validate_inventory(tmp_path))


def test_comments_and_nonwire_strings_do_not_create_sites(tmp_path: Path) -> None:
    _write(
        tmp_path,
        "apps/api/src/curie_api/unrelated.py",
        '# redis.xadd("curie:example", payload)\n'
        'VALUE = "prefix curie:example"\n'
        'def unrelated():\n    return "XADD"\n',
    )
    assert scan_producers(tmp_path) == set()
    assert scan_key_literals(tmp_path) == set()


def test_sandbox_token_has_one_shared_implementation() -> None:
    modules = set(ROOT.glob("apps/*/src/*/sandbox_token.py"))
    modules.update(ROOT.glob("packages/*/src/*/sandbox_token.py"))
    assert modules == {
        ROOT / "packages/curie-internal/src/curie_internal/sandbox_token.py"
    }
