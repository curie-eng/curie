"""Mint upload-only capabilities while the worker stages a static claim."""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any

import yaml
from curie_internal.connector_endpoints import ConnectorEndpointBinding
from plugin_format.connectors import CONNECTORS_FILE, validate_connectors
from plugin_format.deploy_targets import (
    connectors_for_agent,
    restrict_connectors,
    validate_deploy_targets,
)
from plugin_format.validate import DEPLOY_FILE
from plugin_format.yaml_loader import safe_load_unique

from .caller_token import mint_upload

UPLOAD_TOKEN_TTL_SECONDS = 3600


def _read(path: Path) -> Any:
    try:
        return safe_load_unique(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, yaml.YAMLError):
        raise ValueError(f"connector upload declaration {path.name} is unreadable") from None


def mint_claim_upload_tokens(
    signing_key: str, *, bundle: Path | None, endpoints: ConnectorEndpointBinding | None
) -> dict[str, str]:
    if endpoints is None or not any(e.upload_url for e in endpoints.connectors.values()):
        return {}
    if not signing_key.strip():
        raise ValueError("connector uploads require the worker caller signing key")
    if bundle is None:
        raise ValueError("connector uploads require a staged bundle")
    declared, errors = validate_connectors(_read(bundle / CONNECTORS_FILE))
    if errors or declared is None:
        raise ValueError("connector upload declarations are invalid")
    deploy_path = bundle / DEPLOY_FILE
    if deploy_path.exists() or deploy_path.is_symlink():
        targets, errors = validate_deploy_targets(_read(deploy_path))
        if errors or targets is None:
            raise ValueError("connector upload deploy targets are invalid")
        declared = restrict_connectors(declared, connectors_for_agent(targets, endpoints.agent))
    exp = int(time.time()) + UPLOAD_TOKEN_TTL_SECONDS
    return {
        name: mint_upload(signing_key, agent=endpoints.agent, connector=name, exp=exp)
        for name, spec in declared.connectors.items()
        if spec.is_hosted
        and name in endpoints.connectors
        and endpoints.connectors[name].upload_url is not None
    }
