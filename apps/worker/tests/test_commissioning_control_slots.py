"""Free per-launch slots of the measured Pod template; derivation, not ownership."""

from __future__ import annotations

import inspect
from copy import deepcopy

import pytest
from curie_worker import commissioning_control_slots as cs
from curie_worker.protected_kubernetes_projection import parse_projection
from curie_worker.protected_launch_server import ProtectedLaunchServer

from .protected_configuration_vectors import canonical
from .test_protected_kubernetes_projection import projection_value

LAUNCH = "7777aaaa-7777-4777-8777-777777777777"
HEX = "7777aaaa777747778777777777777777"


def projection():
    return parse_projection(canonical(projection_value()))


def pod():
    slots = cs.PodSlots(cs.ControlSlot(LAUNCH), "session/example", "https://api.example.com/h")
    return cs.expected_pod(projection(), slots)


def test_names_derive_from_the_launch_id():
    slot = cs.ControlSlot(LAUNCH)
    assert (slot.pod_name, slot.secret_name, slot.service_name) == (
        "pl-p-" + HEX,
        "pl-c-" + HEX,
        "pl-s-" + HEX,
    )
    for kind, name in (("pod", slot.pod_name), ("secret", slot.secret_name)):
        assert cs.slot_for_name(name, kind) == slot
    assert cs.slot_for_name(slot.service_name, "service") == slot


def test_launch_mechanics_use_the_same_names():
    # The guard and the launch server must agree on the derivation; a renamed
    # launch object would be refused as off the template.
    source = inspect.getsource(ProtectedLaunchServer)
    for prefix in (cs.POD_PREFIX, cs.SECRET_PREFIX, cs.SERVICE_PREFIX):
        assert f'"{prefix}" + suffix' in source
    assert "suffix = UUID(launch_id).hex" in source


@pytest.mark.parametrize(
    ("name", "kind"),
    [
        ("pl-c-" + HEX, "pod"),
        ("pl-p-" + HEX, "secret"),
        ("pl-p-" + HEX.upper(), "pod"),
        ("pl-p-" + HEX[:-1], "pod"),
        ("pl-p-" + HEX + "0", "pod"),
        ("xl-p-" + HEX, "pod"),
        (None, "pod"),
    ],
)
def test_names_off_the_policy_refuse(name, kind):
    with pytest.raises(cs.ControlSlotInvalid):
        cs.slot_for_name(name, kind)


@pytest.mark.parametrize("launch", [LAUNCH.upper(), HEX, "", 7])
def test_launch_id_must_be_canonical(launch):
    with pytest.raises(cs.ControlSlotInvalid):
        cs.ControlSlot(launch)


def test_slots_round_trip_through_the_template():
    rendered = pod()
    extracted = cs.pod_slots(rendered)
    assert extracted.slot == cs.ControlSlot(LAUNCH)
    assert (extracted.session_id, extracted.history_ref) == (
        "session/example",
        "https://api.example.com/h",
    )
    assert cs.expected_pod(projection(), extracted) == rendered
    assert repr(extracted) == "<PodSlots>"


def _env(p, name):
    return next(e for e in p["spec"]["containers"][0]["env"] if e["name"] == name)


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p["metadata"].update(name="pl-p-" + "8" * 32),
        lambda p: p["metadata"]["labels"].update({cs.LAUNCH_LABEL: LAUNCH.upper()}),
        lambda p: p["metadata"].pop("labels"),
        lambda p: p["metadata"].update(name=None),
        lambda p: p["spec"]["containers"].append(deepcopy(p["spec"]["containers"][0])),
        lambda p: p["spec"].update(containers=[]),
        lambda p: p["spec"]["containers"][0]["env"].append(
            {"name": "CURIE_SESSION_ID", "value": "forged"}
        ),
        lambda p: p["spec"]["containers"][0]["env"].remove(_env(p, "CURIE_HISTORY_REF")),
        lambda p: _env(p, "CURIE_SESSION_ID").update(value=""),
        lambda p: _env(p, "CURIE_SESSION_ID").update(value=7),
        lambda p: _env(p, "CURIE_HISTORY_REF").update(value="x" * 8193),
        lambda p: p.update(spec=None),
    ],
)
def test_ambiguous_or_missing_slots_refuse(mutate):
    candidate = pod()
    mutate(candidate)
    with pytest.raises(cs.ControlSlotInvalid) as raised:
        cs.pod_slots(candidate)
    assert raised.value.__cause__ is None and raised.value.__context__ is None


def test_extraction_reads_only_slots_so_the_template_decides_the_rest():
    candidate = pod()
    candidate["spec"]["containers"][0]["image"] = "registry.example.com/other@sha256:" + "0" * 64
    candidate["spec"]["initContainers"] = [deepcopy(candidate["spec"]["containers"][0])]
    extracted = cs.pod_slots(candidate)
    assert cs.expected_pod(projection(), extracted) != candidate
