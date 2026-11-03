"""Protected operator file reading for HTTP identity initializers.

@spec HTTP-INVOCATION-EXECUTION-20.
Local temporary files only; Kubernetes Secret-volume custody is not qualified here.
"""

from __future__ import annotations

import importlib
import os
from pathlib import Path
from typing import Any

import pytest

NAME = "api-runtime.json"


def api() -> Any:
    """@spec HTTP-INVOCATION-EXECUTION-20: fail during execution, never collection."""
    name = "curie_internal.operator_file"
    try:
        module = importlib.import_module(name)
    except ModuleNotFoundError as exc:
        assert exc.name != name, "operator file reader is not implemented"
        raise
    for symbol in ("OperatorFileRefusal", "read_operator_json", "decode_key_material"):
        assert hasattr(module, symbol), f"operator file reader missing {symbol}"
    return module


def write(directory: Path, raw: bytes | str, *, mode: int = 0o440, name: str = NAME) -> Path:
    """@spec HTTP-INVOCATION-EXECUTION-20: one protected operator file."""
    path = directory / name
    if path.exists() and not path.is_symlink():
        path.chmod(0o600)
    path.write_bytes(raw.encode() if isinstance(raw, str) else raw)
    path.chmod(mode)
    return path


def refusal(module: Any, directory: str, *, name: str = NAME) -> str:
    """@spec HTTP-INVOCATION-EXECUTION-20: return the fixed stage of one refusal."""
    with pytest.raises(module.OperatorFileRefusal) as caught:
        module.read_operator_json(directory, name)
    assert caught.value.__cause__ is None and caught.value.__suppress_context__
    assert str(caught.value) == caught.value.stage
    return caught.value.stage


def test_reads_strict_json_from_a_protected_regular_file(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: 0440 and 0400 both satisfy custody checks."""
    module = api()
    for mode in (0o440, 0o400, 0o600, 0o640):
        write(tmp_path, '{"schema_version":1,"items":["x"]}', mode=mode)
        assert module.read_operator_json(str(tmp_path), NAME) == {
            "schema_version": 1, "items": ["x"],
        }


def test_follows_a_secret_volume_link_inside_the_directory(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: Secret volumes expose files through ..data."""
    module = api()
    data = tmp_path / "..2026_10_27"
    data.mkdir()
    write(data, '{"schema_version":1}')
    (tmp_path / "..data").symlink_to(data.name)
    (tmp_path / NAME).symlink_to("..data/" + NAME)
    assert module.read_operator_json(str(tmp_path), NAME) == {"schema_version": 1}


@pytest.mark.parametrize("mode", [0o660, 0o444, 0o442, 0o604, 0o401, 0o620])
def test_refuses_group_writable_or_other_accessible_files(tmp_path: Path, mode: int) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: no group write and no other-user bits."""
    module = api()
    write(tmp_path, '{"schema_version":1}', mode=mode)
    assert refusal(module, str(tmp_path)) == "unsafe_file"


def test_refuses_missing_directory_file_and_non_regular_entries(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: a selection must initialize completely."""
    module = api()
    assert refusal(module, "") == "unconfigured_directory"
    assert refusal(module, str(tmp_path / "absent")) == "unconfigured_directory"
    assert refusal(module, str(tmp_path)) == "unsafe_file"
    (tmp_path / NAME).mkdir()
    assert refusal(module, str(tmp_path)) == "unsafe_file"
    os.rmdir(tmp_path / NAME)
    os.mkfifo(tmp_path / NAME, 0o440)
    assert refusal(module, str(tmp_path)) == "unsafe_file"


def test_refuses_oversized_files_before_decoding(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: at most 1,048,576 bytes."""
    module = api()
    padding = " " * (1_048_576 - len('{"a":1}'))
    write(tmp_path, '{"a":1}' + padding)
    assert module.read_operator_json(str(tmp_path), NAME) == {"a": 1}
    write(tmp_path, '{"a":1}' + padding + " ")
    assert refusal(module, str(tmp_path)) == "unsafe_file"


@pytest.mark.parametrize(
    "raw",
    [
        b'{"a":1,"a":1}',
        b'{"a":NaN}',
        b'{"a":Infinity}',
        b'{"a":-Infinity}',
        b'{"a":"\xff"}',
        b"\xef\xbb\xbf{}",
        b'{"a":1} trailing',
        b"",
        b'{"a":"\\ud800"}',
    ],
)
def test_refuses_noncanonical_json_documents(tmp_path: Path, raw: bytes) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: strict UTF-8 JSON, no duplicate or nonfinite."""
    module = api()
    write(tmp_path, raw)
    assert refusal(module, str(tmp_path)) == "invalid_document"


def test_refusals_never_name_paths_or_values(tmp_path: Path) -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: fixed stage, never supplied material."""
    module = api()
    write(tmp_path, '{"SyntheticSecretValue":NaN}')
    with pytest.raises(module.OperatorFileRefusal) as caught:
        module.read_operator_json(str(tmp_path), NAME)
    rendered = repr(caught.value) + str(caught.value) + repr(caught.value.args)
    assert "SyntheticSecretValue" not in rendered and str(tmp_path) not in rendered


def test_decodes_only_canonical_32_byte_key_material() -> None:
    """@spec HTTP-INVOCATION-EXECUTION-20: unpadded canonical base64url of 32 bytes."""
    module = api()
    material = bytes(range(32))
    canonical = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8"
    assert module.decode_key_material(canonical) == material
    for invalid in (
        canonical + "=",
        canonical[:-1] + "9",
        canonical[:-1],
        canonical + "AA",
        "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh+",
        " " + canonical,
        b"x" * 32,
        None,
        32,
    ):
        with pytest.raises(ValueError) as caught:
            module.decode_key_material(invalid)
        assert str(caught.value) == "invalid_key_material" and caught.value.__cause__ is None
