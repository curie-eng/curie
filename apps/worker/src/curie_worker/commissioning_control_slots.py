"""The per-launch slots the guard leaves free in the measured Pod template.

@spec PROTECTED-COMMISSIONING-8. The projection fixes every Pod field except
five per-launch values, the inputs of ``render_pod``: the launch ID, the Pod
and Secret names, the session ID and the history reference. Under the
``launch-id-names-v1`` slot policy the three object names derive from the
launch ID exactly as the launch mechanics name them, so a Pod's slots reduce to
its launch ID plus two delivery strings. The guard extracts these from an
untrusted Pod, renders the template from them and requires the Pod to equal
it; a Pod whose slots do not round trip is off the template.

A slot is a name, not ownership: which launch owns a name is the launch
registry's record, never the guard's.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from aci_protocol import BootEnv

from .protected_kubernetes_projection import Projection, render_pod
from .protected_launch_records import _require, _text, safe_validation

POD_PREFIX = "pl-p-"
SECRET_PREFIX = "pl-c-"
SERVICE_PREFIX = "pl-s-"
LAUNCH_LABEL = "curietech.ai/protected-launch"
_NAME = re.compile(r"pl-([pcs])-([0-9a-f]{32})")
_KIND = {"p": POD_PREFIX, "c": SECRET_PREFIX, "s": SERVICE_PREFIX}
_SESSION = BootEnv.env_key("session_id")
_HISTORY = BootEnv.env_key("history_ref")


class ControlSlotInvalid(ValueError):
    """Fixed refusal; never echoes a name, label or environment value."""

    def __init__(self) -> None:
        super().__init__("Invalid protected control slot")


@dataclass(frozen=True, slots=True)
class ControlSlot:
    """The object names one launch ID yields; derivation, not ownership."""

    launch_id: str

    @safe_validation(ControlSlotInvalid)
    def __post_init__(self) -> None:
        _require(type(self.launch_id) is str and str(UUID(self.launch_id)) == self.launch_id)

    @property
    def _suffix(self) -> str:
        return UUID(self.launch_id).hex

    @property
    def pod_name(self) -> str:
        return POD_PREFIX + self._suffix

    @property
    def secret_name(self) -> str:
        return SECRET_PREFIX + self._suffix

    @property
    def service_name(self) -> str:
        return SERVICE_PREFIX + self._suffix


@safe_validation(ControlSlotInvalid)
def slot_for_name(name: object, kind: str) -> ControlSlot:
    """The slot whose ``kind`` (pod, secret, service) object has this name."""
    prefix = {"pod": POD_PREFIX, "secret": SECRET_PREFIX, "service": SERVICE_PREFIX}[kind]
    if type(name) is not str:
        raise ControlSlotInvalid()
    match = _NAME.fullmatch(name)
    if match is None:
        raise ControlSlotInvalid()
    _require(_KIND[match[1]] == prefix)
    return ControlSlot(str(UUID(hex=match[2])))


@dataclass(frozen=True, slots=True, repr=False)
class PodSlots:
    """All five free values of one Pod, as extracted; not yet checked against it."""

    slot: ControlSlot
    session_id: str
    history_ref: str

    def __repr__(self) -> str:
        return "<PodSlots>"


@safe_validation(ControlSlotInvalid)
def pod_slots(pod: object) -> PodSlots:
    """Extract the free values of an untrusted Pod for template rendering.

    Only the name, the launch label and two runner environment values are read.
    Every one must be present exactly once; nothing here accepts the Pod.
    """
    if type(pod) is not dict:
        raise ControlSlotInvalid()
    metadata, spec = pod.get("metadata"), pod.get("spec")
    if type(metadata) is not dict or type(spec) is not dict:
        raise ControlSlotInvalid()
    slot = slot_for_name(metadata.get("name"), "pod")
    labels = metadata.get("labels")
    _require(type(labels) is dict and labels.get(LAUNCH_LABEL) == slot.launch_id)
    containers = spec.get("containers")
    if type(containers) is not list or len(containers) != 1 or type(containers[0]) is not dict:
        raise ControlSlotInvalid()
    env = containers[0].get("env")
    if type(env) is not list:
        raise ControlSlotInvalid()
    values: dict[str, list[Any]] = {_SESSION: [], _HISTORY: []}
    for entry in env:
        _require(type(entry) is dict)
        if entry.get("name") in values:
            values[entry["name"]].append(entry.get("value"))
    _require(all(len(found) == 1 for found in values.values()))
    session, history = values[_SESSION][0], values[_HISTORY][0]
    _text(session, 8192)
    _text(history, 8192)
    return PodSlots(slot, session, history)


@safe_validation(ControlSlotInvalid)
def expected_pod(projection: Projection, slots: PodSlots) -> dict[str, Any]:
    """The measured template filled with these slots; the only admissible Pod."""
    _require(type(projection) is Projection and type(slots) is PodSlots)
    return render_pod(
        projection,
        launch_id=slots.slot.launch_id,
        pod_name=slots.slot.pod_name,
        secret_name=slots.slot.secret_name,
        session_id=slots.session_id,
        history_ref=slots.history_ref,
    )
