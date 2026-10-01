"""Export the plugin-format models to a canonical JSON Schema document.

Committed and drift-checked exactly like the ACI schema. Run as
``python -m plugin_format.schema_export`` to rewrite the committed schema.
"""

import json
from pathlib import Path
from typing import Any

from pydantic.json_schema import models_json_schema

from .connector_lock import ConnectorLockEntry, ConnectorLockFile, RunnerLockEntry
from .connectors import (
    ConnectorBuild,
    ConnectorsFile,
    ConnectorSpec,
    RunnerSpec,
    SecretRef,
)
from .deploy_targets import DeployTarget, DeployTargetsFile
from .models import (
    ApprovalGate,
    ApprovalPolicy,
    Author,
    HookDefinition,
    HookMatcherConfig,
    McpConfig,
    McpServer,
    PluginManifest,
    SkillFrontmatter,
    ToolPolicy,
    TriggerDeclaration,
)

_MODELS = (
    PluginManifest,
    Author,
    SkillFrontmatter,
    McpServer,
    McpConfig,
    HookMatcherConfig,
    HookDefinition,
    TriggerDeclaration,
    ApprovalPolicy,
    ApprovalGate,
    ToolPolicy,
    # The Curie-only root files (connectors.yaml, connectors.lock.yaml,
    # deploy.yaml). They are frozen bundle shapes like the manifest, so they
    # ride the same drift gate (#1128).
    ConnectorsFile,
    ConnectorSpec,
    ConnectorBuild,
    SecretRef,
    RunnerSpec,
    ConnectorLockFile,
    ConnectorLockEntry,
    RunnerLockEntry,
    DeployTargetsFile,
    DeployTarget,
)

SCHEMA_ID = "https://curietech.ai/schemas/plugin-format.schema.json"


def schema_path() -> Path:
    """The committed schema file location inside this package."""

    return Path(__file__).resolve().parents[2] / "schema" / "plugin-format.schema.json"


def build_schema() -> dict[str, Any]:
    """Build the combined JSON Schema document for the plugin-format models."""

    _, top = models_json_schema(
        [(model, "validation") for model in _MODELS],
        ref_template="#/$defs/{model}",
    )
    doc: dict[str, Any] = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "$id": SCHEMA_ID,
        "title": "Claude Code Plugin Format",
    }
    doc.update(top)
    return doc


def render_schema() -> str:
    """Render the canonical schema string (sorted keys, trailing newline)."""

    return json.dumps(build_schema(), indent=2, sort_keys=True) + "\n"


def write_schema() -> Path:
    """Write the canonical schema to its committed location and return the path."""

    path = schema_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render_schema(), encoding="utf-8")
    return path


if __name__ == "__main__":
    written = write_schema()
    print(f"wrote {written}")
