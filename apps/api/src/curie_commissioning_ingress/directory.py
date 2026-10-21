"""The provisioner-owned commissioning ingress directory, @spec PROTECTED-COMMISSIONING-2.

The directory shares the SOURCE-11 ``manifest.json`` and ``ca.pem`` grammar of
the runtime directory writer and adds three commissioning files:

- ``commissioning_bootstrap.json``: the commissioning bootstrap B;
- ``guard_config.json``: the guard configuration G holding ``sha256(B)``;
- ``commissioning_ingress.json``: exactly ``schema_version`` 1, the selected
  ``campaign`` (``campaign_id``, ``campaign_generation``), the
  ``candidate_enqueue`` principal (``credential_ref``, ``username``,
  ``password``) and 1 to 64 synthetic signed ``sources``, each exactly
  ``source`` (as in the permit), ``hook``, ``secret`` and ``target_ref``.

The production ``bootstrap.json``, ``enqueue.json`` and ``source_writer.json``
are never opened. Possessing the directory authorizes nothing: the ingress
still needs the issuer-written permit, a running journal and the guard. Every
defect is one ``CommissioningDirectoryInvalid`` without detail.
"""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass, field
from typing import Any

from curie_api.protected_runtime_files import _DIRECTORY_FLAGS, _read_file
from curie_protected_hooks.authority_records import (
    _CREDENTIAL,
    Manifest,
    _object,
    _pairs,
    _reject_number,
    _scalar,
    parse_manifest,
)
from curie_protected_hooks.broker_transport import trusted_ca_pem
from curie_protected_hooks.commissioning_bootstrap import (
    CommissioningBootstrap,
    parse_commissioning_bootstrap,
)
from curie_protected_hooks.runtime_directory import CA_FILE, MANIFEST_FILE, MAX_FILE_BYTES
from curie_protected_hooks.worker_transport import WorkerCredential

# @spec PROTECTED-COMMISSIONING-2
BOOTSTRAP_FILE = "commissioning_bootstrap.json"
GUARD_FILE = "guard_config.json"
INGRESS_FILE = "commissioning_ingress.json"
_HOOK = re.compile(r"[A-Za-z0-9_-]{1,64}", re.ASCII)
_SECRET_BYTES = (16, 8192)
_MAX_SOURCES = 64
_CAMPAIGN: dict[str, Any] = {"campaign_id": "uuid", "campaign_generation": "generation"}
_SOURCE: dict[str, Any] = {
    "source_id": "uuid",
    "generation": "generation",
    "policy_fingerprint": "sha256",
}


class CommissioningDirectoryInvalid(Exception):
    """A missing, unreadable or invalid commissioning file, @spec PROTECTED-COMMISSIONING-2."""


@dataclass(frozen=True, slots=True, repr=False)
class SignedSource:
    """One synthetic source with its signing secret, @spec PROTECTED-COMMISSIONING-4."""

    source: dict[str, Any]
    hook: str
    secret: str = field(repr=False)
    target_ref: str


@dataclass(frozen=True, slots=True, repr=False)
class CommissioningDirectory:
    """The parsed directory; never echoed, @spec PROTECTED-COMMISSIONING-2."""

    manifest: Manifest
    ca_pem: str
    bootstrap: CommissioningBootstrap
    guard_config: bytes
    campaign: dict[str, str]
    credential_ref: dict[str, str]
    credential: WorkerCredential
    sources: dict[str, SignedSource]


def _require(condition: bool) -> None:
    if not condition:
        raise CommissioningDirectoryInvalid()


def _strict_json(raw: bytes) -> Any:
    _require(len(raw) <= MAX_FILE_BYTES)
    return json.loads(
        raw.decode("utf-8"),
        object_pairs_hook=_pairs,
        parse_float=_reject_number,
        parse_constant=_reject_number,
    )


def _source(value: Any) -> SignedSource:
    _require(type(value) is dict and value.keys() == {"source", "hook", "secret", "target_ref"})
    _object(value["source"], _SOURCE)
    hook, secret = value["hook"], value["secret"]
    _require(type(hook) is str and _HOOK.fullmatch(hook) is not None)
    _require(type(secret) is str and secret.isascii())
    _require(_SECRET_BYTES[0] <= len(secret) <= _SECRET_BYTES[1])
    _scalar(value["target_ref"], "opaque_ref")
    return SignedSource(dict(value["source"]), hook, secret, value["target_ref"])


def _ingress(
    raw: bytes,
) -> tuple[dict[str, str], dict[str, str], WorkerCredential, dict[str, SignedSource]]:
    value = _strict_json(raw)
    _require(
        type(value) is dict
        and value.keys() == {"schema_version", "campaign", "candidate_enqueue", "sources"}
    )
    _scalar(value["schema_version"], "version")
    _object(value["campaign"], _CAMPAIGN)
    principal = value["candidate_enqueue"]
    _require(
        type(principal) is dict and principal.keys() == {"credential_ref", "username", "password"}
    )
    _object(principal["credential_ref"], _CREDENTIAL)
    # A broker session authenticates as the user its credential reference names.
    _require(principal["username"] == principal["credential_ref"]["id"])
    _require(type(principal["password"]) is str)
    credential = WorkerCredential(principal["username"], principal["password"])
    sources = value["sources"]
    _require(type(sources) is list and 1 <= len(sources) <= _MAX_SOURCES)
    parsed = [_source(source) for source in sources]
    by_id = {source.source["source_id"]: source for source in parsed}
    _require(len(by_id) == len(parsed))
    return value["campaign"], principal["credential_ref"], credential, by_id


def load_commissioning_directory(directory: str) -> CommissioningDirectory:
    """Every commissioning file under one opened directory, read afresh.

    Blocking; the caller runs it off the event loop. @spec PROTECTED-COMMISSIONING-2.
    """
    try:
        directory_fd = os.open(directory, _DIRECTORY_FLAGS)
        try:
            manifest_raw = _read_file(directory_fd, MANIFEST_FILE)
            ca_raw = _read_file(directory_fd, CA_FILE)
            bootstrap_raw = _read_file(directory_fd, BOOTSTRAP_FILE)
            guard_raw = _read_file(directory_fd, GUARD_FILE)
            ingress_raw = _read_file(directory_fd, INGRESS_FILE)
        finally:
            os.close(directory_fd)
        campaign, reference, credential, sources = _ingress(ingress_raw)
        return CommissioningDirectory(
            manifest=parse_manifest(manifest_raw),
            ca_pem=trusted_ca_pem(ca_raw.decode("ascii")),
            bootstrap=parse_commissioning_bootstrap(bootstrap_raw),
            guard_config=guard_raw,
            campaign=campaign,
            credential_ref=reference,
            credential=credential,
            sources=sources,
        )
    except Exception:  # noqa: BLE001  Any read or parse failure is the one invalid directory refusal.
        raise CommissioningDirectoryInvalid() from None
