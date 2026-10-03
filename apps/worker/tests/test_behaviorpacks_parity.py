"""Field-name parity between the API write-side pack DTOs and the worker read-side
value objects (#500).

Behavior-pack config is authored/validated on the API (`schemas.py`, mutable
`list` fields) and re-parsed at bind time by the worker
(`behaviorpacks.py`, frozen `tuple` fields). The two representations differ on
purpose (list vs tuple, `SettingConfig` vs frozen `Setting`), but their FIELD
NAMES must agree: `BehaviorPacks.from_config` parses totally with pydantic's
default `extra="ignore"`, so a field added on the write side but not the read
side is silently dropped at runtime -- no 422, a dead knob. This gate compares
field-name sets per pair so that divergence fails loudly instead.
"""

from __future__ import annotations

import pytest
from curie_api.schemas import agents as api
from curie_worker import behaviorpacks as worker

# (API write-side model, worker read-side model). An added-but-unpaired pack
# surfaces here too: forgetting to list it leaves it ungated, but the eight
# below are the full BehaviorPacksConfig surface today.
_PAIRS = [
    (api.LoadPackConfig, worker.LoadPack),
    (api.TipsPackConfig, worker.TipsPack),
    (api.GreetingPackConfig, worker.GreetingPack),
    (api.HelpPackConfig, worker.HelpPack),
    (api.SettingConfig, worker.Setting),
    (api.SettingsPackConfig, worker.SettingsPack),
    (api.NavPackConfig, worker.NavPack),
    (api.BehaviorPacksConfig, worker.BehaviorPacks),
]


@pytest.mark.parametrize("api_model, worker_model", _PAIRS, ids=lambda m: m.__name__)
def test_pack_field_names_match_across_lanes(api_model, worker_model) -> None:
    api_fields = set(api_model.model_fields)
    worker_fields = set(worker_model.model_fields)
    assert api_fields == worker_fields, (
        f"{api_model.__name__} (API) and {worker_model.__name__} (worker) have "
        f"diverged field sets; only-on-API fields are silently dropped by "
        f"BehaviorPacks.from_config. Symmetric difference: "
        f"{sorted(api_fields ^ worker_fields)}"
    )


def test_top_level_pack_keys_are_all_paired() -> None:
    # Guard against a NEW pack added to BehaviorPacksConfig without a parity pair
    # above: every sub-pack field of the top-level config must be represented.
    top_level_fields = set(api.BehaviorPacksConfig.model_fields)
    paired = {
        "load": True,
        "tips": True,
        "greeting": True,
        "help": True,
        "settings": True,
        "nav": True,
    }
    assert top_level_fields == set(paired), (
        "BehaviorPacksConfig gained/lost a sub-pack; add it to _PAIRS and to this "
        f"guard. Fields: {sorted(top_level_fields)}"
    )
