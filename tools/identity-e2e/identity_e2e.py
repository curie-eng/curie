#!/usr/bin/env python3
"""Drive real HTTP identity turns against a disposable Curie install.

Entry point for ``curie dev identity-e2e -- <action> [flags]``. Standard
library only on the host; product code that must sign or adopt runs inside the
release's own API pod, so the harness exercises the deployed build rather than
a copy of it. Every action names its target explicitly with ``--context``,
``--namespace`` and ``--release``; there is no default kube context.

Actions:

1. ``up`` overlays HTTP identity onto an existing install: a TLS test identity
   provider (``test_idp.py``) inside the namespace, the API's protected
   ``api-runtime.json`` Secret with an email receiver registration, a helm
   upgrade with ``--reuse-values`` that mounts it and supplies the connector
   caller signing key and the email adapter's reply secret, the tenant's HTTP
   selection, an invocation role filter and one email binding plus channel
   token per ``--agent``.
2. ``send`` signs an email receiver proof for exactly the submitted body, as
   the mail adapter does, and posts ``/channels/turns``.
3. ``observe`` reads the newest sandbox claim's connector caller token for an
   agent and prints its decoded payload with the signature redacted, plus a
   short sha256 fingerprint of the token that the fake model's
   ``[fake:connector-call]`` reply lines carry for the header they sent.
4. ``prove --scenario <name>`` runs a scripted scenario from
   ``identity_scenarios.py`` and writes JSON evidence.
5. ``down`` removes what ``up`` created and restores the helm values it
   changed, then verifies the removal.

State (generated keys, ids, channel tokens) lives in ``--state-dir`` with
mode 0700 and 0600 files. No action prints a key, bearer, proof or token.
See docs/testing/identity-e2e.md.
"""

from __future__ import annotations

import argparse
import ast
import base64
import contextlib
import copy
import hashlib
import json
import os
import re
import secrets
import shutil
import sys
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Iterator, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from identity_assets import (
    CREDENTIAL_ID,
    DEFAULT_ROSTER,
    DOMAIN,
    INBOX_DOMAIN,
    ISSUER,
    LOOKUP_PORT,
    OWNER_LABEL,
    SINK_PORT,
    ed25519_pair,
    get_path,
    make_certificates,
    provider_manifests,
    restored_values,
    role_filter,
    runtime_document,
    secret_manifest,
    set_path,
)
from identity_target import (
    ADAPTER,
    ADOPT_SCRIPT,
    API_PORT,
    HERE,
    REPO,
    ROLE_FILTER_SCRIPT,
    SIGN_SCRIPT,
    Api,
    HarnessError,
    Target,
    b64url,
    b64url_decode,
    http,
    log,
    summarize_event,
    write_private,
)

DEFAULT_CHART = REPO / "charts" / "curie"
STATE_FILE = "state.json"
STATE_VERSION = 1
CALLER_TOKEN_ENV = "CURIE_CONNECTOR_CALLER_TOKEN"
CLAIM_RESOURCE = "sandboxclaims.extensions.agents.x-k8s.io"
AGENT_LABEL = "curietech.ai/agent"
# A hosted connector's objects, as plugin_format.connector_render renders them:
# `_labels` puts the release in part-of and the object name (release, agent and
# connector joined by `-mcp-`) in name, and both appliers stamp the owning
# agent's name in the owner label (connector_reconcile.OWNER_LABEL). The caller
# proxy container names its connector in its environment.
CONNECTOR_COMPONENT_LABEL = "app.kubernetes.io/component"
CONNECTOR_COMPONENT = "mcp-connector"
CONNECTOR_RELEASE_LABEL = "app.kubernetes.io/part-of"
CONNECTOR_OWNER_LABEL = "curie.dev/connector-owner"
CONNECTOR_JOIN = "-mcp-"
CALLER_PROXY_CONTAINER = "caller-proxy"
CALLER_PROXY_CONNECTOR_ENV = "CURIE_CALLER_PROXY_CONNECTOR"
FINGERPRINT_HEX = 16
REPLY_TEXT_LIMIT = 2000

# `kubectl logs --timestamps` puts an RFC 3339 stamp with nanoseconds first.
_LOG_STAMP = re.compile(
    r"(?P<stamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?P<fraction>\.\d+)?"
    r"(?P<zone>Z|[+-]\d{2}:\d{2})\s+"
)
# The proxy's logging prefix: `LEVEL name ` from its basicConfig format, or the
# logging default `LEVEL:name:`.
_PROXY_LOG_PREFIX = re.compile(r"[A-Z]+[: ]curie_connector_proxy(?:\.[A-Za-z0-9_.]+)?[: ]")
_QUOTED = r"'(?:[^'\\]|\\.)*'|\"(?:[^\"\\]|\\.)*\""
_PROXY_DECISION = re.compile(
    r"caller=(?P<caller>\S+) outcome=(?P<outcome>[a-z_]+) method=(?P<method>\S+) "
    rf"path=(?P<path>{_QUOTED})(?P<rest>.*)"
)
_PROXY_SUFFIX = re.compile(rf"\s+(?P<key>[a-z_]+)=(?P<value>{_QUOTED}|\S+)")


def slug(name: str) -> str:
    """A lowercase address-safe label for an agent name."""

    value = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    if not value:
        raise HarnessError(f"agent name {name!r} has no address-safe characters")
    return value[:40]


def slack_channel_for(name: str) -> str:
    """A synthetic, stable Slack channel id for an agent `up` deploys."""

    return "C0IE2E" + hashlib.sha256(name.encode("utf-8")).hexdigest()[:8].upper()


def decode_caller_token(token: str) -> dict[str, Any]:
    """The payload of a ``cct.<payload>.<signature>`` token, signature redacted."""

    parts = token.split(".")
    if len(parts) != 3 or parts[0] not in {"cct", "ccr"}:
        raise HarnessError("the connector caller token is not a cct/ccr token")
    try:
        payload = json.loads(b64url_decode(parts[1]))
    except (ValueError, UnicodeDecodeError):
        raise HarnessError("the connector caller token payload is not JSON") from None
    if not isinstance(payload, dict):
        raise HarnessError("the connector caller token payload is not an object")
    return {"prefix": parts[0], "payload": payload, "signature": "redacted"}


def proxied_connector(deployment: Mapping[str, Any]) -> str | None:
    """The connector a Deployment's caller proxy names, or None without a proxy."""

    containers = deployment.get("spec", {}).get("template", {}).get("spec", {}).get("containers")
    for container in containers or []:
        if container.get("name") != CALLER_PROXY_CONTAINER:
            continue
        for env in container.get("env") or []:
            if env.get("name") == CALLER_PROXY_CONNECTOR_ENV and isinstance(env.get("value"), str):
                return str(env["value"])
    return None


def caller_fingerprint(token: str) -> str:
    """The first 16 hex of the token's sha256, as the fake model prints for a sent header."""

    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:FINGERPRINT_HEX]


def split_log_stamp(line: str) -> tuple[float | None, str]:
    """Unix seconds of a leading ``kubectl logs --timestamps`` stamp, and the rest."""

    match = _LOG_STAMP.match(line)
    if match is None:
        return None, line
    # Nanoseconds are more than datetime parses; microseconds are enough here.
    fraction = (match["fraction"] or "")[:7]
    zone = "+00:00" if match["zone"] == "Z" else match["zone"]
    return epoch(match["stamp"] + fraction + zone), line[match.end() :]


def parse_proxy_decision(line: str) -> dict[str, Any] | None:
    """One caller proxy decision line, or None for any other line.

    The line is the proxy's per-request log, optionally after a kubectl
    timestamp: ``caller=<agent|-> outcome=<admitted|refusal> method=<m>
    path=<repr>`` then, in any order, ``route=<repr>``, ``grant=<reason>`` and
    ``principal=<present|absent>``. ``principal`` is None when the line has none
    (a refusal, a route decision, or a proxy that predates the field).
    """

    _, rest = split_log_stamp(line.strip())
    prefix = _PROXY_LOG_PREFIX.match(rest)
    if prefix is None:
        return None
    match = _PROXY_DECISION.fullmatch(rest[prefix.end() :].rstrip())
    if match is None:
        return None
    try:
        path = ast.literal_eval(match["path"])
    except (ValueError, SyntaxError):
        return None
    suffixes: dict[str, Any] = {}
    remaining = match["rest"]
    while remaining.strip():
        suffix = _PROXY_SUFFIX.match(remaining)
        if suffix is None:
            return None
        value = suffix["value"]
        if value[:1] in {"'", '"'}:
            try:
                value = ast.literal_eval(value)
            except (ValueError, SyntaxError):
                return None
        suffixes[suffix["key"]] = value
        remaining = remaining[suffix.end() :]
    decision: dict[str, Any] = {
        "caller": match["caller"],
        "outcome": match["outcome"],
        "method": match["method"],
        "path": path,
        "principal": suffixes.pop("principal", None),
    }
    for key in ("route", "grant"):
        if key in suffixes:
            decision[key] = suffixes[key]
    return decision


def epoch(stamp: str) -> float:
    """Unix seconds for an RFC 3339 timestamp; 0 when it does not parse."""

    try:
        parsed = datetime.fromisoformat(stamp.replace("Z", "+00:00"))
    except ValueError:
        return 0.0
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.timestamp()


def turn_completed(replies: Sequence[Mapping[str, Any]]) -> bool:
    return any((reply.get("event") or {}).get("event") == "turn.completed" for reply in replies)


def turn_summary(replies: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Whether the worker finished the turn, its outcome and the reply text it sent."""

    events = [reply.get("event") or {} for reply in replies]
    completed = [event for event in events if event.get("event") == "turn.completed"]
    return {
        "completed": bool(completed),
        "outcome": completed[-1].get("outcome") if completed else None,
        "texts": [event["text"] for event in events if isinstance(event.get("text"), str)],
    }


# --- state ---------------------------------------------------------------------


class State:
    """The JSON state file in a mode 0700 directory."""

    def __init__(self, directory: Path) -> None:
        self.directory = directory
        self.path = directory / STATE_FILE
        self.data: dict[str, Any] = {}

    def ensure_directory(self) -> None:
        self.directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.directory, 0o700)

    def exists(self) -> bool:
        return self.path.is_file()

    def load(self) -> dict[str, Any]:
        if not self.exists():
            raise HarnessError(f"no harness state in {self.directory}; run `up` first")
        mode = self.path.stat().st_mode & 0o777
        if mode & 0o077:
            raise HarnessError(f"{self.path} is readable by other users; expected mode 0600")
        self.data = json.loads(self.path.read_text())
        if self.data.get("version") != STATE_VERSION:
            raise HarnessError("the state file was written by another harness version")
        return self.data

    def save(self) -> None:
        self.ensure_directory()
        write_private(self.path, json.dumps(self.data, indent=2, sort_keys=True).encode())


# --- the harness ---------------------------------------------------------------------


class Harness:
    """Actions over one target and one state directory."""

    def __init__(self, target: Target, state: State) -> None:
        self.target = target
        self.state = state
        target.private_config = str(state.directory / "curie-config")

    @property
    def data(self) -> dict[str, Any]:
        return self.state.data

    @property
    def prefix(self) -> str:
        return f"{self.target.release}-identity-e2e"

    @property
    def provider_host(self) -> str:
        return f"{self.prefix}-idp.{self.target.namespace}.svc"

    def _check_target(self) -> None:
        recorded = self.data.get("target")
        current = {
            "context": self.target.context,
            "namespace": self.target.namespace,
            "release": self.target.release,
        }
        if recorded is not None and recorded != current:
            raise HarnessError(f"the state directory belongs to {recorded}, not {current}")
        self.data["target"] = current

    @contextlib.contextmanager
    def api(self) -> Iterator[Api]:
        key = self.target.platform_key()
        with self.target.forward(f"{self.target.fullname}-api", API_PORT, "/health") as base:
            yield Api(base, key)

    def records(self) -> list[dict[str, Any]]:
        with self.target.forward(f"{self.prefix}-idp", SINK_PORT, "/healthz") as base:
            status, payload = http("GET", base + "/records")
        if status != 200:
            raise HarnessError(f"the provider records answered {status}")
        return list(payload["records"])

    # up -----------------------------------------------------------------------

    def up(
        self,
        agents: Sequence[str],
        bundle: Path | None,
        roster: Mapping[str, Any],
        *,
        with_role_filter: bool = False,
    ) -> dict[str, Any]:
        if not agents:
            raise HarnessError("up needs at least one --agent")
        self.state.ensure_directory()
        if self.state.exists():
            self.state.load()
        else:
            self.data.update(version=STATE_VERSION, agents={}, helm_previous={}, sends={})
        self._check_target()
        target = self.target
        keys = self.data.setdefault("keys", {})
        keys.setdefault("receiver", b64url(secrets.token_bytes(32)))
        keys.setdefault("context", b64url(secrets.token_bytes(32)))
        keys.setdefault("bearer", secrets.token_urlsafe(32))
        keys.setdefault("adapter_secret", secrets.token_urlsafe(32))
        self.data["roster"] = copy.deepcopy(dict(roster))
        self.data.update(
            fullname=target.fullname,
            issuer=ISSUER,
            domain=DOMAIN,
            lookup_url=f"https://{self.provider_host}:{LOOKUP_PORT}/lookup",
        )
        self.state.save()

        hosts = [
            f"{self.prefix}-idp",
            self.provider_host,
            f"{self.provider_host}.cluster.local",
        ]
        if not (self.state.directory / "server.pem").is_file():
            log("creating the private CA and provider certificate")
            make_certificates(self.state.directory, hosts)

        log("applying the test identity provider")
        manifests = provider_manifests(
            prefix=self.prefix,
            release=target.release,
            image=target.api_image(),
            roster=roster,
            certificate=(self.state.directory / "server.pem").read_bytes(),
            private_key=(self.state.directory / "server.key").read_bytes(),
            bearer=keys["bearer"],
            adapter_secret=keys["adapter_secret"],
        )
        target.kubectl(
            "apply",
            "-f",
            "-",
            input_text=json.dumps({"apiVersion": "v1", "kind": "List", "items": manifests}),
        )
        target.rollout(f"{self.prefix}-idp", timeout="300s")

        # Hosted connectors render their proxy's verify key when an agent is
        # deployed, so the release must carry the caller key pair, and the API
        # must have rolled to it, before any agent is deployed.
        self._ensure_caller_pair()

        with self.api() as api:
            self._agents(api, agents, bundle)
            tenant_id = self._tenant()
            self._bindings(api, tenant_id)
        selection = {
            "mode": "http",
            "issuer": ISSUER,
            "lookup_url": self.data["lookup_url"],
            "credential_ref": {"id": CREDENTIAL_ID, "generation": 1},
            "max_identity_age_ms": 60_000,
            "max_membership_age_ms": 60_000,
            "max_clock_skew_ms": 5_000,
        }
        log("adopting the tenant HTTP selection")
        adoption = target.pod_python(ADOPT_SCRIPT, {"tenant_id": tenant_id, "selection": selection})
        recorded = self.data.setdefault("adoption", adoption)
        if adoption.get("created_row"):
            recorded["created_row"] = True
        self.state.save()
        with self.api() as api:
            for name, agent in self.data["agents"].items():
                self._access_policy(api, name, agent)
        self.data["role_filter"] = bool(self.data.get("role_filter")) or with_role_filter
        for name, agent in self.data["agents"].items():
            # The filter is opt-in: it makes every binding of the agent require
            # HTTP identity, so the worker then refuses a `cluster message`
            # turn to that agent before any claim.
            wanted = role_filter() if self.data["role_filter"] else agent.get("previous_filter")
            if not self.data["role_filter"] and "previous_filter" not in agent:
                continue
            result = target.pod_python(
                ROLE_FILTER_SCRIPT,
                {"tenant_id": tenant_id, "agent_id": agent["agent_id"], "filter": wanted},
            )
            if "previous_filter" not in agent:
                agent["previous_filter"] = result["previous"]
            log(f"invocation filter for {name}: {result['action']}")
        self.state.save()

        registration = self.data.setdefault(
            "registration",
            {
                "schema_version": 1,
                "receiver_id": str(uuid.uuid4()),
                "generation": 1,
                "tenant_id": tenant_id,
                "channel_identity_id": str(uuid.uuid4()),
                "provider_installation_id": str(uuid.uuid4()),
                "binding_ids": [],
                "domains": [
                    {
                        "id": str(uuid.uuid4()),
                        "authority": "",
                        "kind": "email_domain",
                        "key": DOMAIN,
                    }
                ],
            },
        )
        registration["binding_ids"] = sorted(
            agent["binding_id"] for agent in self.data["agents"].values()
        )
        document = runtime_document(
            tenant_id=tenant_id,
            selection=selection,
            bearer=keys["bearer"],
            registration=registration,
            receiver_key=keys["receiver"],
            context_key=keys["context"],
        )
        log("writing the API runtime Secret")
        runtime = secret_manifest(
            f"{self.prefix}-api-runtime",
            target.release,
            {
                "api-runtime.json": json.dumps(document, sort_keys=True).encode(),
                "provider-ca.pem": (self.state.directory / "ca.pem").read_bytes(),
            },
        )
        target.kubectl("apply", "-f", "-", input_text=json.dumps(runtime))
        self.state.save()

        self._helm_overlay()
        log("rolling the api and worker")
        target.rollout(target.component("api"), target.component("worker"), restart=True)

        with self.api() as api:
            for name, agent in self.data["agents"].items():
                agent.update(api.mint_channel_token(agent["address"]))
                log(f"minted the channel token for {name}")
        self.state.save()
        return {
            "action": "up",
            "release": target.release,
            "namespace": target.namespace,
            "tenant_id": tenant_id,
            "issuer": ISSUER,
            "lookup_url": self.data["lookup_url"],
            "adoption": {k: v for k, v in adoption.items() if k != "previous"},
            "agents": {
                name: {k: agent[k] for k in ("agent_id", "binding_id", "address", "created")}
                for name, agent in self.data["agents"].items()
            },
            "roster": sorted(roster.get("senders", {})),
            "state_dir": str(self.state.directory),
        }

    def _access_policy(self, api: Api, name: str, agent: dict[str, Any]) -> None:
        """An HTTP turn needs a stored access policy: its version is provenance.

        Version 0 means none was ever written, and the HTTP lane then answers
        503 identity_authority_unavailable. A teamless policy changes no
        admission decision.
        """

        policy = api.ok("GET", f"/agents/{agent['agent_id']}/access-policy")
        if int(policy["version"]) == 0:
            api.ok(
                "PUT",
                f"/agents/{agent['agent_id']}/access-policy",
                {"invocation_team_id": None, "approver_team_id": None, "expected_version": 0},
            )
            agent["created_policy"] = True
            log(f"wrote a teamless access policy for {name}")

    def _agents(self, api: Api, names: Sequence[str], bundle: Path | None) -> None:
        existing = api.agents()
        agents = self.data["agents"]
        for name in names:
            if name not in existing:
                if bundle is None:
                    raise HarnessError(
                        f"agent {name} does not exist; deploy it or pass --bundle to deploy one"
                    )
                log(f"deploying {name} from {bundle}")
                result = self.target.curie(
                    "cluster",
                    "deploy",
                    "--plugin-dir",
                    str(bundle),
                    "--agent",
                    name,
                    "--slack-channel",
                    slack_channel_for(name),
                    "--json",
                )
                if result.returncode != 0:
                    tail = (result.stderr or result.stdout).strip().splitlines()[-5:]
                    raise HarnessError(f"curie cluster deploy {name} failed: {' | '.join(tail)}")
                agents.setdefault(name, {})["created"] = True
                existing = api.agents()
                if name not in existing:
                    raise HarnessError(f"agent {name} is still missing after deploy")
            entry = agents.setdefault(name, {})
            entry.setdefault("created", False)
            entry["agent_id"] = str(existing[name]["id"])
        self.state.save()

    def _tenant(self) -> str:
        ids = [agent["agent_id"] for agent in self.data["agents"].values()]
        rows = self.target.sql(
            (
                "SELECT DISTINCT tenant_id::text AS tenant_id FROM curie.agents "
                "WHERE id::text = ANY(:ids)",
                {"ids": ids},
            )
        )[0]
        if len(rows) != 1:
            raise HarnessError("the agents do not share one tenant")
        tenant = str(rows[0]["tenant_id"])
        self.data["tenant_id"] = tenant
        return tenant

    def _bindings(self, api: Api, tenant_id: str) -> None:
        for name, agent in self.data["agents"].items():
            address = f"{slug(name)}@{INBOX_DOMAIN}"
            endpoint = f"http://{self.provider_host}:{SINK_PORT}/reply/{slug(name)}"
            api.ok(
                "POST",
                f"/agents/{agent['agent_id']}/channels",
                {"kind": "email", "address": address, "adapter": ADAPTER, "endpoint": endpoint},
            )
            rows = self.target.sql(
                (
                    "SELECT id::text AS id FROM curie.agent_channels WHERE agent_id::text = :a "
                    "AND kind = 'email' AND address = :address AND adapter = :adapter",
                    {"a": agent["agent_id"], "address": address, "adapter": ADAPTER},
                )
            )[0]
            if len(rows) != 1:
                raise HarnessError(f"expected one email binding for {name}, found {len(rows)}")
            agent.update(address=address, binding_id=rows[0]["id"], label=slug(name))
            log(f"email binding for {name}: {address}")
        self.state.save()

    def _ensure_caller_pair(self) -> None:
        """Set the connector caller key pair and roll the API to it, once."""

        target = self.target
        values = target.helm_values()
        keys = self.data["keys"]
        previous = self.data.setdefault("helm_previous", {})
        caller_set = bool(
            get_path(values, "connectorCaller.signingKey")
            or get_path(values, "connectorCaller.existingSecret")
        )
        if caller_set and "connectorCaller.signingKey" not in previous:
            return  # the operator's own pair stays; down has nothing to restore
        if "caller_signing" not in keys:
            keys["caller_signing"], keys["caller_verify"] = ed25519_pair(self.state.directory)
            self.state.save()
        changes = {
            "connectorCaller.signingKey": keys["caller_signing"],
            "connectorCaller.verifyKey": keys["caller_verify"],
        }
        overlay: dict[str, Any] = {}
        for path, value in changes.items():
            if path not in previous:
                previous[path] = get_path(values, path)
            set_path(overlay, path, value)
        self.state.save()
        if all(get_path(values, path) == value for path, value in changes.items()):
            return
        self._helm_upgrade(overlay, "caller-pair")
        log("rolling the api to the caller key pair")
        target.rollout(target.component("api"), restart=True)

    def _helm_overlay(self) -> None:
        target = self.target
        values = target.helm_values()
        keys = self.data["keys"]
        previous = self.data.setdefault("helm_previous", {})
        overlay: dict[str, Any] = {}
        changes: dict[str, Any] = {
            "api.httpIdentity.existingSecret": f"{self.prefix}-api-runtime",
            "api.httpIdentity.providerCaBundle": True,
        }
        if get_path(values, "worker.adapterCredentialsExistingSecret"):
            raise HarnessError(
                "worker.adapterCredentialsExistingSecret is set; the harness cannot add its "
                "adapter secret to a BYO Secret"
            )
        changes[f"worker.adapterCredentials.{ADAPTER}"] = keys["adapter_secret"]
        for path, value in changes.items():
            if path not in previous:
                previous[path] = get_path(values, path)
            set_path(overlay, path, value)
        self.state.save()
        self._helm_upgrade(overlay, "overlay")

    def _helm_upgrade(self, overlay: Mapping[str, Any], label: str, *, reset: bool = False) -> None:
        """Upgrade with ``overlay`` merged over the release's values, or replacing them."""

        path = self.state.directory / f"helm-{label}.json"
        write_private(path, json.dumps(overlay).encode())
        mode = "--reset-values" if reset else "--reuse-values"
        log(f"helm upgrade {mode} ({label})")
        try:
            self.target.helm(
                "upgrade",
                self.target.release,
                str(self.target.chart),
                mode,
                "-f",
                str(path),
                "--timeout",
                "15m",
            )
        finally:
            path.unlink(missing_ok=True)

    # send -------------------------------------------------------------------

    def ensure_token(self, api: Api, name: str) -> str:
        agent = self.agent_entry(name)
        if not agent.get("token") or agent.get("expires_at", 0) < time.time() + 300:
            agent.update(api.mint_channel_token(agent["address"]))
            self.state.save()
        return str(agent["token"])

    def agent_entry(self, name: str) -> dict[str, Any]:
        agent = self.data.get("agents", {}).get(name)
        if agent is None:
            raise HarnessError(f"agent {name} is not in the harness state; run `up --agent {name}`")
        return agent  # type: ignore[no-any-return]

    def send(
        self,
        name: str,
        sender: str,
        text: str,
        conversation: str | None = None,
        delivery: str | None = None,
    ) -> dict[str, Any]:
        agent = self.agent_entry(name)
        delivery_id = delivery or f"<{uuid.uuid4().hex}@identity-e2e.example.test>"
        conversation_id = conversation or f"<{uuid.uuid4().hex}@identity-e2e.example.test>"
        body = json.dumps(
            {
                "kind": "email",
                "address": agent["address"],
                "delivery_id": delivery_id,
                "conversation_id": conversation_id,
                "author": sender,
                "text": text,
                "reply_ref": "",
                "attachments": [],
            },
            separators=(",", ":"),
        ).encode()
        registration = self.data["registration"]
        signed = self.target.pod_python(
            SIGN_SCRIPT,
            {
                "key": self.data["keys"]["receiver"],
                "proof": {
                    "schema_version": 1,
                    "receiver_id": registration["receiver_id"],
                    "receiver_generation": registration["generation"],
                    "binding_id": agent["binding_id"],
                    "sender": sender.lower(),
                    "delivery_id": delivery_id,
                    "body_sha256": hashlib.sha256(body).hexdigest(),
                },
            },
        )
        sent_at = time.time()
        with self.api() as api:
            token = self.ensure_token(api, name)
            status, payload = http(
                "POST",
                api.base + "/channels/turns",
                raw=body,
                headers={"X-API-Key": token, "X-Curie-Receiver-Proof": signed["proof"]},
            )
        record = {
            "agent": name,
            "sender": sender.lower(),
            "delivery_id": delivery_id,
            "conversation_id": conversation_id,
            "sent_at": sent_at,
            "http_status": status,
            "response": payload,
        }
        self.data.setdefault("sends", {}).setdefault(name, []).append(
            {k: record[k] for k in ("delivery_id", "conversation_id", "sent_at", "http_status")}
        )
        del self.data["sends"][name][:-20]
        self.state.save()
        return record

    # observe ------------------------------------------------------------------

    def claims(self, name: str, since: float) -> list[dict[str, Any]]:
        items = self.target.kubectl_json("get", CLAIM_RESOURCE, "-l", f"{AGENT_LABEL}={name}")[
            "items"
        ]
        found = []
        for item in items:
            created = item["metadata"]["creationTimestamp"]
            stamp = epoch(created)
            if stamp >= since:
                found.append(
                    {"name": item["metadata"]["name"], "created": created, "created_epoch": stamp}
                )
        return sorted(found, key=lambda claim: claim["created_epoch"])

    def caller_token(self, claim: str) -> str | None:
        result = self.target.kubectl("get", "secret", f"{claim}-tokens", "-o", "json", check=False)
        if result.returncode != 0:
            return None
        encoded = json.loads(result.stdout).get("data", {}).get(CALLER_TOKEN_ENV)
        return None if not encoded else base64.b64decode(encoded).decode("ascii")

    def observe(
        self,
        name: str,
        *,
        since: float | None = None,
        wait: float = 180,
        conversation: str | None = None,
        reply_wait: float | None = None,
    ) -> dict[str, Any]:
        """The newest claim's caller token for ``name``, and the turn's outcome.

        The claim and its token are awaited for ``wait`` seconds. With no
        ``since``, the window starts at the agent's last ``send`` (for
        ``conversation`` when given) and the turn's email reply events are
        awaited too, until ``turn.completed`` arrives or ``reply_wait`` (default
        ``wait``) runs out. That budget starts once the token is found, so a
        slow claim does not shorten it; with no token the replies are read once.
        """

        self.agent_entry(name)
        if since is None:
            sends = self.data.get("sends", {}).get(name, [])
            matching = [s for s in sends if conversation in (None, s["conversation_id"])]
            if matching:
                since = matching[-1]["sent_at"] - 2
                conversation = matching[-1]["conversation_id"]
            else:
                since = 0.0
        deadline = time.monotonic() + wait
        claim: dict[str, Any] | None = None
        token: str | None = None
        while True:
            claims = self.claims(name, since)
            if claims:
                claim = claims[-1]
                token = self.caller_token(claim["name"])
                if token is not None:
                    break
            if time.monotonic() > deadline:
                break
            time.sleep(3)
        result: dict[str, Any] = {
            "agent": name,
            "since": since,
            "conversation_id": conversation,
            "claim": None if claim is None else claim["name"],
            "claim_created": None if claim is None else claim["created"],
            "caller_token_present": token is not None,
        }
        reply_deadline = deadline
        if token is not None:
            decoded = decode_caller_token(token)
            result.update(
                prefix=decoded["prefix"],
                claims=decoded["payload"],
                signature="redacted",
                caller_fingerprint=caller_fingerprint(token),
            )
            reply_deadline = time.monotonic() + (wait if reply_wait is None else reply_wait)
        replies = self.replies(name, since, conversation)
        while conversation is not None and not turn_completed(replies):
            if time.monotonic() > reply_deadline:
                break
            time.sleep(3)
            replies = self.replies(name, since, conversation)
        result["replies"] = replies
        result["turn"] = turn_summary(replies)
        return result

    def replies(
        self, name: str, since: float, conversation: str | None = None
    ) -> list[dict[str, Any]]:
        """Reply events the sink recorded for ``name`` since ``since``."""

        label = self.agent_entry(name)["label"]
        out = []
        for record in self.records():
            if record.get("type") != "reply" or record.get("label") != label:
                continue
            event = record.get("event") or {}
            target = event.get("target") if isinstance(event, dict) else None
            if conversation is not None and (
                not isinstance(target, dict) or target.get("conversation_id") != conversation
            ):
                continue
            at = str(record.get("at", ""))
            if epoch(at) >= since:
                # Room for one `connector-call` line per connector in the text.
                out.append(
                    {
                        "at": at,
                        "status": record.get("status"),
                        "event": summarize_event(event, REPLY_TEXT_LIMIT),
                    }
                )
        return out

    # hosted connectors ----------------------------------------------------------

    def _connector_deployments(self, name: str) -> dict[str, dict[str, Any]]:
        """The agent's hosted connector Deployments, by connector name."""

        self.agent_entry(name)
        release = self.target.release
        selector = ",".join(
            (
                f"{CONNECTOR_COMPONENT_LABEL}={CONNECTOR_COMPONENT}",
                f"{CONNECTOR_RELEASE_LABEL}={release}",
                f"{CONNECTOR_OWNER_LABEL}={name}",
            )
        )
        found: dict[str, dict[str, Any]] = {}
        for item in self.target.kubectl_json("get", "deployment", "-l", selector)["items"]:
            deployment = str(item["metadata"]["name"])
            connector = proxied_connector(item)
            prefix = f"{release}-{name}{CONNECTOR_JOIN}"
            if connector is None and deployment.startswith(prefix):
                connector = deployment.removeprefix(prefix)
            if not connector:
                raise HarnessError(f"cannot tell which connector Deployment {deployment} serves")
            if connector in found:
                raise HarnessError(f"{name} has two Deployments for connector {connector}")
            found[connector] = item
        return found

    def connectors(self, name: str) -> list[str]:
        """The hosted connectors deployed for ``name``, sorted."""

        return sorted(self._connector_deployments(name))

    def proxy_decisions(self, name: str, since: float) -> list[dict[str, Any]]:
        """Caller proxy decisions logged by ``name``'s connector pods at or after ``since``.

        Each is a :func:`parse_proxy_decision` entry plus the ``connector`` whose
        pod logged it and the log stamp ``at``. Kept: every decision whose
        caller is ``name``, and every refusal whatever its caller, so a step
        sees a refusal it did not expect.
        """

        stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(since))
        decisions: list[tuple[float, dict[str, Any]]] = []
        for connector, deployment in sorted(self._connector_deployments(name).items()):
            if proxied_connector(deployment) is None:
                raise HarnessError(
                    f"connector {connector} of {name} has no {CALLER_PROXY_CONTAINER} "
                    "container; is connectorCaller configured?"
                )
            labels = deployment["spec"]["selector"]["matchLabels"]
            selector = ",".join(f"{key}={value}" for key, value in sorted(labels.items()))
            pods = self.target.kubectl_json("get", "pod", "-l", selector)["items"]
            for pod in pods:
                pod_name = str(pod["metadata"]["name"])
                result = self.target.kubectl(
                    "logs",
                    pod_name,
                    "-c",
                    CALLER_PROXY_CONTAINER,
                    "--timestamps",
                    f"--since-time={stamp}",
                    check=False,
                )
                if result.returncode != 0:
                    log(f"no caller proxy log from {pod_name}: exit {result.returncode}")
                    continue
                for line in result.stdout.splitlines():
                    at, rest = split_log_stamp(line)
                    if at is not None and at < since:
                        continue
                    parsed = parse_proxy_decision(line)
                    if parsed is None:
                        continue
                    if parsed["caller"] != name and parsed["outcome"] == "admitted":
                        continue
                    parsed.update(connector=connector, at=line[: len(line) - len(rest)].strip())
                    decisions.append((since if at is None else at, parsed))
        return [entry for _, entry in sorted(decisions, key=lambda pair: pair[0])]

    # down ---------------------------------------------------------------------

    def down(self) -> dict[str, Any]:
        self.state.load()
        self._check_target()
        target = self.target
        report: dict[str, Any] = {"action": "down", "removed": [], "residual": []}
        agents = self.data.get("agents", {})
        tenant_id = self.data.get("tenant_id")
        with self.api() as api:
            existing = api.agents()
            for name, agent in agents.items():
                if name not in existing:
                    continue
                if agent.get("created"):
                    # An agent with an active deployment refuses deletion.
                    for deployment in api.ok("GET", f"/deployments?agent_id={agent['agent_id']}"):
                        if deployment.get("status") == "active":
                            api.ok("DELETE", f"/deployments/{deployment['id']}")
                    api.ok("DELETE", f"/agents/{agent['agent_id']}")
                    report["removed"].append(f"agent {name}")
                    continue
                if agent.get("address"):
                    status, payload = api.call(
                        "DELETE",
                        f"/agents/{agent['agent_id']}/channels?kind=email&address="
                        f"{urllib.request.quote(agent['address'])}&adapter={ADAPTER}",
                    )
                    if status in (204, 404):
                        report["removed"].append(f"email binding {agent['address']}")
                    else:
                        report["residual"].append(f"email binding {agent['address']}: {status}")
                if tenant_id and "previous_filter" in agent:
                    target.pod_python(
                        ROLE_FILTER_SCRIPT,
                        {
                            "tenant_id": tenant_id,
                            "agent_id": agent["agent_id"],
                            "filter": agent["previous_filter"],
                        },
                    )
                    report["removed"].append(f"invocation filter on {name} restored")
                if agent.get("created_policy"):
                    report["residual"].append(
                        f"teamless access policy row on {name} (the API has no delete)"
                    )
        previous = self.data.get("helm_previous", {})
        if previous:
            restore = restored_values(target.helm_values(), previous)
            self._helm_upgrade(restore, "restore", reset=True)
            report["removed"].append("helm values restored: " + ", ".join(sorted(previous)))
        target.kubectl(
            "delete",
            "deployment,service,configmap,secret",
            "-l",
            f"{OWNER_LABEL}={target.release}",
            "--ignore-not-found",
            "--wait=true",
        )
        report["removed"].append("provider and runtime objects")
        adoption = self.data.get("adoption", {})
        if tenant_id and adoption.get("created_row"):
            try:
                target.sql(
                    (
                        "DELETE FROM curie.tenant_identity_providers WHERE tenant_id::text = :t",
                        {"t": tenant_id},
                    )
                )
                report["removed"].append("tenant HTTP selection (row created by up)")
            except HarnessError as exc:
                report["residual"].append(f"tenant HTTP selection: {exc}")
        elif adoption:
            report["residual"].append(
                "tenant HTTP selection: the row predated up and is left as adopted"
            )
        target.rollout(target.component("api"), target.component("worker"), restart=True)
        report["verified"] = self._verify_removed(previous)
        for name in (STATE_FILE, "ca.pem", "ca.key", "server.pem", "server.key"):
            (self.state.directory / name).unlink(missing_ok=True)
        shutil.rmtree(self.state.directory / "curie-config", ignore_errors=True)
        with contextlib.suppress(OSError):
            self.state.directory.rmdir()
        return report

    def _verify_removed(self, previous: Mapping[str, Any]) -> dict[str, Any]:
        target = self.target
        leftover = target.kubectl(
            "get",
            "deployment,service,configmap,secret",
            "-l",
            f"{OWNER_LABEL}={target.release}",
            "-o",
            "name",
        ).stdout.split()
        if leftover:
            raise HarnessError(f"objects remain after down: {leftover}")
        values = target.helm_values()
        mismatched = [path for path, value in previous.items() if get_path(values, path) != value]
        if mismatched:
            raise HarnessError(f"helm values not restored: {mismatched}")
        return {"objects_remaining": 0, "helm_values_restored": sorted(previous)}


# --- command line ------------------------------------------------------------------


def _target(args: argparse.Namespace) -> Target:
    return Target(
        context=args.context,
        namespace=args.namespace,
        release=args.release,
        curie_bin=args.curie_bin,
        chart=Path(args.chart),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="curie dev identity-e2e --",
        description="Drive real HTTP identity turns against a disposable Curie install.",
    )
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--context", required=True, help="kube context (never defaulted)")
    common.add_argument("--namespace", required=True)
    common.add_argument("--release", required=True)
    common.add_argument("--state-dir", required=True, type=Path)
    common.add_argument(
        "--curie-bin", default=os.environ.get("CURIE_BIN") or shutil.which("curie") or "curie"
    )
    common.add_argument(
        "--chart",
        default=str(DEFAULT_CHART),
        help="chart for the helm upgrade (default: this checkout's)",
    )
    actions = parser.add_subparsers(dest="action", required=True)

    up = actions.add_parser("up", parents=[common], help="overlay HTTP identity on the install")
    up.add_argument("--agent", action="append", required=True, dest="agents")
    up.add_argument("--bundle", type=Path, help="deploy a missing --agent from this bundle")
    up.add_argument("--roster", type=Path, help="a roster JSON replacing the default one")
    up.add_argument(
        "--role-filter",
        action="store_true",
        help="also set an issuer-group invocation filter on every agent (its bindings then "
        "require HTTP identity, and non-HTTP turns to it are refused)",
    )

    send = actions.add_parser("send", parents=[common], help="post one signed email turn")
    send.add_argument("--agent", required=True)
    send.add_argument("--sender", required=True)
    send.add_argument("--text", required=True)
    send.add_argument("--conversation")
    send.add_argument("--delivery-id")

    observe = actions.add_parser("observe", parents=[common], help="decode the caller token")
    observe.add_argument("--agent", required=True)
    observe.add_argument("--conversation")
    observe.add_argument("--since", type=float, help="unix seconds; default the last send")
    observe.add_argument("--wait", type=float, default=180, help="seconds to await the token")
    observe.add_argument(
        "--reply-wait",
        type=float,
        help="seconds to await the reply once the token is found (default: --wait)",
    )

    prove = actions.add_parser("prove", parents=[common], help="run a scripted scenario")
    prove.add_argument("--scenario", required=True)
    prove.add_argument("--evidence", type=Path)
    prove.add_argument("--flag-agent", help="default: the first agent `up` registered")
    prove.add_argument("--control-agent", help="default: the second agent `up` registered")
    prove.add_argument("--sender", default="alice@example.test")
    prove.add_argument("--wait", type=float, default=240)

    actions.add_parser("down", parents=[common], help="remove what up created")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    target = _target(args)
    harness = Harness(target, State(args.state_dir.resolve()))
    try:
        if args.action == "up":
            roster = DEFAULT_ROSTER if args.roster is None else json.loads(args.roster.read_text())
            bundle = None if args.bundle is None else args.bundle.resolve()
            result: Any = harness.up(args.agents, bundle, roster, with_role_filter=args.role_filter)
        elif args.action == "send":
            harness.state.load()
            result = harness.send(
                args.agent, args.sender, args.text, args.conversation, args.delivery_id
            )
        elif args.action == "observe":
            harness.state.load()
            result = harness.observe(
                args.agent,
                since=args.since,
                wait=args.wait,
                conversation=args.conversation,
                reply_wait=args.reply_wait,
            )
        elif args.action == "prove":
            import identity_scenarios

            harness.state.load()
            evidence = identity_scenarios.run(args.scenario, harness, args)
            if args.evidence is not None:
                args.evidence.write_text(json.dumps(evidence, indent=2, sort_keys=True) + "\n")
            print(json.dumps(evidence, indent=2, sort_keys=True))
            return 0 if evidence.get("passed") else 1
        else:
            result = harness.down()
    except HarnessError as exc:
        print(json.dumps({"action": args.action, "error": str(exc)}), file=sys.stderr)
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.path.insert(0, str(HERE))
    # identity_scenarios imports this module by name; share one copy so its
    # `except HarnessError` catches what this module raises.
    sys.modules.setdefault("identity_e2e", sys.modules[__name__])
    sys.exit(main())
