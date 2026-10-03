"""The plugin-format compat gate: committed schema must match the models."""

import inspect
from types import ModuleType

from plugin_format import connector_lock, connectors, deploy_targets, models
from plugin_format.schema_export import build_schema, render_schema, schema_path
from pydantic import BaseModel

# Every module that defines the shape of a file a bundle carries: plugin.json,
# SKILL.md frontmatter and .mcp.json (models), then the Curie-only root files
# connectors.yaml, connectors.lock.yaml and deploy.yaml (issue #1128).
_FILE_SHAPE_MODULES: tuple[ModuleType, ...] = (
    models,
    connectors,
    connector_lock,
    deploy_targets,
)


def _file_shape_models() -> list[str]:
    return sorted(
        name
        for module in _FILE_SHAPE_MODULES
        for name, obj in vars(module).items()
        if inspect.isclass(obj) and issubclass(obj, BaseModel) and obj.__module__ == module.__name__
    )


def test_committed_json_schema_is_current() -> None:
    committed = schema_path().read_text(encoding="utf-8")
    assert render_schema() == committed, (
        "plugin-format JSON Schema is stale; run scripts/check-contracts.sh and commit"
    )


def test_every_root_file_model_is_exported() -> None:
    # The drift gate above only sees models the export visits. A model left out
    # of schema_export._MODELS can gain, lose or rename a field with every gate
    # green, so enumerate the defining modules rather than trust a hand list.
    found = _file_shape_models()
    assert "ConnectorSpec" in found and "DeployTarget" in found, found
    exported = set(build_schema()["$defs"])
    missing = [name for name in found if name not in exported]
    assert not missing, (
        f"bundle file-shape models missing from plugin-format.schema.json $defs: {missing}; "
        "add them to plugin_format.schema_export._MODELS and run scripts/check-contracts.sh"
    )
