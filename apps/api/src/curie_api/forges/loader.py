"""Load ``CURIE_FORGES``: the factory's forge config as the API receives it (#4397).

The env value is a `ForgesConfig` JSON document whose credentials name files
(``private_key_file``, ``client_secret_file``, ``token_file``) the chart mounts
from Secrets. A GitHub App key or OAuth client secret file is read here, once,
and its content becomes the secret byte for byte. A static token file is read
here only to refuse a bad one at load; the config keeps the path, and every
operation reads the file again (`StaticTokenCredentialConfig.current_token`),
so a rotated Secret takes effect with no restart (#4630). A secret written
inline in the env JSON is refused, so key material never sits in the
environment. A GitHub entry must use a GitHub App, because
the tracker's marked-comment identity is the App's. A kind this build does not
install is refused at load. Every refusal names the entry and the file, never
the secret.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from curie_api.forges import types
from curie_api.forges.config import ForgesConfig, read_static_token
from curie_api.forges.errors import CredentialUnreadable
from curie_api.forges.registry import check_installed

# Each credential secret field and the file field that must carry it.
_SECRET_FILES = {
    "private_key": "private_key_file",
    "client_secret": "client_secret_file",
    "token": "token_file",
}


def _path(entry: str, field: str, path: Any) -> str:
    if not isinstance(path, str) or not path:
        raise ValueError(f"{entry} credential {field} must be a file path")
    return path


def _read_secret(entry: str, field: str, path: str) -> str:
    try:
        content = Path(path).read_text()
    except OSError:
        raise ValueError(f"{entry} credential {field} {path} cannot be read") from None
    if not content.strip():
        raise ValueError(f"{entry} credential {field} {path} is empty")
    return content


def _check_token_file(entry: str, path: str) -> None:
    """Refuse at load a token file no operation could send; the value is discarded."""

    try:
        read_static_token(path)
    except CredentialUnreadable as refused:
        problem = refused.problem
    else:
        return
    raise ValueError(f"{entry} credential token_file {path} {problem}")


def _credential(entry: str, kind: Any, credential: Any) -> dict[str, Any]:
    if not isinstance(credential, dict):
        raise ValueError(f"{entry} credential must be an object")
    loaded = dict(credential)
    inline = sorted(field for field in _SECRET_FILES if field in loaded)
    if inline:
        raise ValueError(
            f"{entry} credential carries {', '.join(inline)} inline; name a"
            f" {' or '.join(_SECRET_FILES[field] for field in inline)} instead"
        )
    if kind == types.GITHUB and loaded.get("type") != "github_app":
        raise ValueError(f"{entry} is a github entry and must use a github_app credential")
    for secret, file_field in _SECRET_FILES.items():
        if file_field not in loaded:
            continue
        path = _path(entry, file_field, loaded[file_field])
        if file_field == "token_file":
            # Kept as a path: each operation reads the current file.
            _check_token_file(entry, path)
        else:
            loaded[secret] = _read_secret(entry, file_field, loaded.pop(file_field))
    return loaded


def load_forges_json(raw: str) -> ForgesConfig:
    """The `ForgesConfig` of a ``CURIE_FORGES`` value; empty gives an empty config."""

    if not raw.strip():
        return ForgesConfig()
    try:
        document = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("CURIE_FORGES is not valid JSON") from None
    if not isinstance(document, dict):
        raise ValueError("CURIE_FORGES must be a JSON object")
    loaded = dict(document)
    for section, what in (("code_hosts", "code host"), ("trackers", "tracker")):
        entries = loaded.get(section, [])
        if not isinstance(entries, list):
            raise ValueError(f"CURIE_FORGES {section} must be a list")
        resolved = []
        for entry in entries:
            if not isinstance(entry, dict):
                raise ValueError(f"CURIE_FORGES {section} entries must be objects")
            label = f"{what} {entry.get('name')!r}"
            resolved.append(
                {
                    **entry,
                    "credential": _credential(label, entry.get("kind"), entry.get("credential")),
                }
            )
        loaded[section] = resolved
    try:
        config = ForgesConfig.model_validate(loaded)
    except ValidationError as refused:
        # Never echo the input: it now holds the secrets read from the files.
        raise ValueError(_describe(refused)) from None
    check_installed(config)
    return config


def _describe(refused: ValidationError) -> str:
    problems = [
        f"{'.'.join(str(part) for part in error['loc']) or 'CURIE_FORGES'}: {error['msg']}"
        for error in refused.errors(include_input=False, include_url=False)
    ]
    return "CURIE_FORGES is invalid: " + "; ".join(problems)
