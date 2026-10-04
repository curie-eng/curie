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
    # The consumer's ownership protected literals remain declared without migration.
    assert any("curie_worker/consumer.py" in site for site in literals)


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
        (
            "apps/api/src/curie_api/new_producer.py",
            "from typing import Any\n"
            "async def publish(client, stream, payload):\n"
            "    append: Any = client.xadd\n"
            '    await append(stream, {"payload": payload})\n',
        ),
        (
            "apps/api/src/curie_api/new_producer.py",
            "async def publish(client, stream, payload):\n"
            '    await getattr(client, "xadd")(stream, {"payload": payload})\n',
        ),
        (
            "apps/api/src/curie_api/new_producer.py",
            "class Publisher:\n"
            "    def __init__(self, client):\n"
            "        self.publish = client.xadd\n"
            "    async def enqueue(self, stream, payload):\n"
            '        return await self.publish(stream, {"payload": payload})\n',
        ),
        (
            "apps/worker/src/curie_worker/new_producer.py",
            'SCRIPT = f"""return redis.call("XADD", KEYS[1], "*", "payload", ARGV[1])\n'
            '-- {description}"""\n',
        ),
        (
            "cli/src/new_producer.rs",
            'fn publish() { let append = redis::cmd; append("XADD").arg(stream); }\n',
        ),
        (
            "cli/src/new_producer.rs",
            'const SCRIPT: &str = r#"return redis.call("XADD", KEYS[1], "*", '
            '"payload", ARGV[1])"#;\n',
        ),
    ],
    ids=[
        "python",
        "lua",
        "rust",
        "python_annotated_alias",
        "python_getattr",
        "python_class_attribute_alias",
        "lua_in_fstring",
        "rust_bound_alias",
        "lua_in_rust_raw_string",
    ],
)
def test_undeclared_producer_is_rejected(tmp_path: Path, relative: str, source: str) -> None:
    _write(tmp_path, relative, source)
    assert any(relative in site for site in scan_producers(tmp_path))
    assert any(relative in error for error in validate_inventory(tmp_path))


def test_duplicate_producer_in_an_inventoried_scope_is_rejected(tmp_path: Path) -> None:
    relative = "apps/dispatcher/src/curie_dispatcher/queue.py"
    source = (ROOT / relative).read_text(encoding="utf-8")
    statement = "            stream_id = redis_client.xadd(config.stream, fields)\n"
    assert source.count(statement) == 1
    _write(tmp_path, relative, source)
    producers = scan_producers(tmp_path)
    errors = set(validate_inventory(tmp_path))

    _write(tmp_path, relative, source.replace(statement, statement + statement))

    assert len(scan_producers(tmp_path)) == len(producers) + 1
    new_errors = set(validate_inventory(tmp_path)) - errors
    assert any(relative in error for error in new_errors)


@pytest.mark.parametrize(
    "source",
    [
        'KEY = "curie:undeclared:key"\n',
        "KEY = 'curie:undeclared:key'\n",
        'def key(identity):\n    return f"curie:undeclared:{identity}"\n',
        # Existing wire values do not authorize a new construction site.
        'KEY = "curie:runs"\n',
        'KEY = "cu" + "rie:undeclared:key"\n',
        'KEY = f"{\'curie\'}:undeclared:key"\n',
        'KEY = f"cu{\'rie\'}:undeclared:key"\n',
    ],
    ids=[
        "double_quotes",
        "single_quotes",
        "formatted",
        "existing_value_new_site",
        "concatenated_prefix",
        "formatted_static_prefix",
        "formatted_split_prefix",
    ],
)
def test_undeclared_key_literal_is_rejected(tmp_path: Path, source: str) -> None:
    relative = "apps/api/src/curie_api/new_key.py"
    _write(tmp_path, relative, source)
    assert any(relative in site for site in scan_key_literals(tmp_path))
    assert any(relative in error for error in validate_inventory(tmp_path))


def test_duplicate_literal_in_an_inventoried_scope_is_rejected(tmp_path: Path) -> None:
    relative = "apps/worker/src/curie_worker/consumer.py"
    source = (ROOT / relative).read_text(encoding="utf-8")
    statement = 'THREAD_RESET_SET = "curie:thread-reset-requests"\n'
    assert source.count(statement) == 1
    _write(tmp_path, relative, source)
    literals = scan_key_literals(tmp_path)
    errors = set(validate_inventory(tmp_path))

    _write(tmp_path, relative, source.replace(statement, statement + statement))

    assert len(scan_key_literals(tmp_path)) == len(literals) + 1
    new_errors = set(validate_inventory(tmp_path)) - errors
    assert any(relative in error for error in new_errors)


def test_undeclared_rust_key_literal_is_rejected(tmp_path: Path) -> None:
    relative = "cli/src/new_key.rs"
    _write(tmp_path, relative, 'pub const KEY: &str = "curie:undeclared:key";\n')
    assert any(relative in site for site in scan_key_literals(tmp_path))
    assert any(relative in error for error in validate_inventory(tmp_path))


def test_undeclared_rust_concatenated_key_literal_is_rejected(tmp_path: Path) -> None:
    relative = "cli/src/new_key.rs"
    _write(tmp_path, relative, 'pub const KEY: &str = concat!("curie", ":undeclared:key");\n')
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


@pytest.mark.parametrize(
    ("relative", "source"),
    [
        (
            "apps/api/src/curie_api/nonproducer.py",
            "from typing import Any\n"
            "async def read(client, key):\n"
            "    fetch: Any = client.get\n"
            "    return await fetch(key)\n",
        ),
        (
            "apps/api/src/curie_api/nonproducer.py",
            "async def read(client, key):\n"
            '    return await getattr(client, "get")(key)\n',
        ),
        (
            "apps/api/src/curie_api/nonproducer.py",
            "class Reader:\n"
            "    def __init__(self, client):\n"
            "        self.fetch = client.get\n"
            "    async def read(self, key):\n"
            "        return await self.fetch(key)\n",
        ),
        (
            "apps/worker/src/curie_worker/nonproducer.py",
            'SCRIPT = f"""-- redis.call("XADD", KEYS[1], "*", "payload", ARGV[1])\n'
            'return redis.call("GET", KEYS[1]) -- {description}"""\n',
        ),
        (
            "cli/src/nonproducer.rs",
            'fn read() { let fetch = redis::cmd; fetch("GET").arg(key); }\n',
        ),
        (
            "cli/src/nonproducer.rs",
            'const SCRIPT: &str = r#"-- redis.call("XADD", KEYS[1], "*", "payload", ARGV[1])\n'
            'return redis.call("GET", KEYS[1])"#;\n',
        ),
    ],
    ids=[
        "python_annotated_reader",
        "python_getattr_reader",
        "python_class_attribute_reader",
        "lua_fstring_comment",
        "rust_bound_reader",
        "lua_rust_comment",
    ],
)
def test_nonwriters_using_supported_syntax_do_not_create_producer_sites(
    tmp_path: Path, relative: str, source: str
) -> None:
    _write(tmp_path, relative, source)
    assert scan_producers(tmp_path) == set()


def test_sandbox_token_has_one_shared_implementation() -> None:
    source_roots = [ROOT / "runner/src"]
    for parent in ("apps", "packages", "adapters", "tools"):
        source_roots.extend(ROOT.glob(f"{parent}/*/src"))
    modules = {
        path
        for source_root in source_roots
        for path in source_root.rglob("sandbox_token.py")
    }
    assert modules == {
        ROOT / "packages/curie-internal/src/curie_internal/sandbox_token.py"
    }
