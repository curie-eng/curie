"""@spec STARTABILITY-1 through STARTABILITY-6; see ../../docs/STARTABILITY.md."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import re
import sys
from collections.abc import Callable, Mapping
from typing import Any, Never, cast

# @spec STARTABILITY-1
BINDINGS_SQL = """
SELECT a.name AS agent, c.kind, c.address, c.adapter AS identity,
       CASE WHEN jsonb_typeof(a.secrets) = 'object'
            THEN coalesce((SELECT array_agg(k ORDER BY k)
                           FROM jsonb_object_keys(a.secrets) AS k), '{}')
            ELSE '{}' END AS secret_names,
       EXISTS (SELECT 1 FROM curie.deployments d
               JOIN curie.agent_versions v ON v.id = d.version_id AND v.agent_id = a.id
               WHERE d.agent_id = a.id AND d.status = 'active') AS deployed
FROM curie.agents a
JOIN curie.agent_channels c ON c.agent_id = a.id
ORDER BY a.name, c.kind, c.address
"""
# @spec STARTABILITY-5
WORKER_VARIABLES = (
    "CURIE_WARM_POOL",
    "CURIE_AGENT_SANDBOX_POOLS",
    "CURIE_AGENT_CONNECTOR_SECRET_POOLS",
)


def exception_type(exc: Exception) -> str:
    """@spec STARTABILITY-4 STARTABILITY-6."""
    return re.sub(r"[^A-Za-z0-9_]", "", type(exc).__name__)[:80] or "Exception"


def identity_lanes(env: list[dict[str, Any]]) -> dict[str, tuple[str, str]]:
    """@spec STARTABILITY-3."""
    raw = next((e.get("value") for e in env if e["name"] == "CURIE_SLACK_IDENTITIES"), None)
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        return {"default": ("SLACK_APP_TOKEN", "SLACK_BOT_TOKEN")}
    declared = json.loads(raw)
    if not isinstance(declared, list):
        raise ValueError("invalid identity declaration")
    lanes: dict[str, tuple[str, str]] = {}
    for item in declared:
        if not isinstance(item, dict):
            raise ValueError("invalid identity declaration")
        name, app, bot = (item.get(key) for key in ("name", "app_token_env", "bot_token_env"))
        if not isinstance(name, str) or not isinstance(app, str) or not isinstance(bot, str):
            raise ValueError("invalid identity declaration")
        if not all(value.strip() for value in (name, app, bot)):
            raise ValueError("invalid identity declaration")
        if name in lanes:
            raise ValueError("duplicate identity declaration")
        lanes[name] = (app, bot)
    return lanes


def credential_states(
    env: list[dict[str, Any]],
    load_secret: Callable[[str], Mapping[str, str | bytes]],
    *,
    names: set[str] | None = None,
    load_config_map: Callable[[str], Mapping[str, str | bytes]] | None = None,
) -> dict[str, tuple[bool, str]]:
    """@spec STARTABILITY-4 STARTABILITY-5."""
    cache: dict[tuple[str, str], tuple[Mapping[str, str | bytes] | None, str | None]] = {}
    states: dict[str, tuple[bool, str]] = {}
    for entry in env:
        name = entry["name"]
        relevant = (
            name in names
            if names is not None
            else (
                name in ("SLACK_APP_TOKEN", "SLACK_BOT_TOKEN")
                or name.startswith(("CURIE_SLACK_APP_TOKEN__", "CURIE_SLACK_BOT_TOKEN__"))
            )
        )
        if not relevant:
            continue
        kind = "secret" if entry.get("secret") else "config_map"
        ref = entry.get(kind)
        if ref:
            resource, key = ref["name"], ref["key"]
            label = "Secret" if kind == "secret" else "ConfigMap"
            cache_key = kind, resource
            if cache_key not in cache:
                try:
                    loader = load_secret if kind == "secret" else load_config_map
                    if loader is None:
                        raise ValueError("unsupported credential reference")
                    cache[cache_key] = loader(resource), None
                except Exception as exc:  # noqa: BLE001 - reduce external errors without their text
                    cache[cache_key] = None, exception_type(exc)
            values, error = cache[cache_key]
            if error:
                state = False, f"could not read {label} {resource} ({error})"
            elif values is None or key not in values:
                state = False, f"{label} {resource} has no key {key}"
            elif not values[key].strip():
                state = False, f"{label} {resource} key {key} is blank"
            else:
                state = True, "credential is nonblank"
        else:
            value = entry.get("value")
            state = (
                (True, "credential is nonblank")
                if isinstance(value, str) and value.strip()
                else (False, f"dispatcher value {name} is blank")
            )
        states[name] = state
    return states


def judge(
    bindings: list[Mapping[str, Any]],
    worker_env: Mapping[str, str | None],
    lanes: Mapping[str, tuple[bool, str]] | set[str],
    pools: Mapping[str, str],
    templates: Mapping[str, set[str]],
    *,
    identity_lanes: Mapping[str, tuple[str, str]] | None = None,
    claim: Callable[[str, Mapping[str, str], str, frozenset[str], frozenset[str]], str],
    reserved: Callable[[str], bool],
    marker: str,
) -> list[dict[str, Any]]:
    """@spec STARTABILITY-1 STARTABILITY-2 STARTABILITY-3 STARTABILITY-6."""
    base_pool = worker_env.get("CURIE_WARM_POOL") or "curie-runner-pool"

    def listed(key: str) -> frozenset[str]:
        """@spec STARTABILITY-2."""
        return frozenset(s.strip() for s in (worker_env.get(key) or "").split(",") if s.strip())

    def reason_for(binding: Mapping[str, Any]) -> str | None:
        """@spec STARTABILITY-1 STARTABILITY-2 STARTABILITY-3."""
        identity = binding["identity"] or "default"
        if binding["kind"] == "slack":
            credentials: tuple[str, str] | None
            if identity_lanes is None:
                suffix = "" if identity == "default" else "_" + identity.upper().replace("-", "_")
                credentials = ("SLACK_APP_TOKEN" + suffix, "SLACK_BOT_TOKEN" + suffix)
            else:
                credentials = identity_lanes.get(identity)
            if credentials is None:
                return f"Slack identity {identity} has no declaration"
            for lane in credentials:
                state = (
                    (lane in lanes, "variable is absent")
                    if isinstance(lanes, set)
                    else (lanes.get(lane, (False, "variable is absent")))
                )
                if not state[0]:
                    return f"Slack identity {identity}: {lane}: {state[1]}"
        if not binding["deployed"]:
            return "the agent has no active deployment"
        names = sorted(name for name in binding["secret_names"] or [] if not reserved(name))
        try:
            pool = claim(
                base_pool,
                {marker: ",".join(names)} if names else {},
                binding["agent"],
                listed("CURIE_AGENT_SANDBOX_POOLS"),
                listed("CURIE_AGENT_CONNECTOR_SECRET_POOLS"),
            )
        except Exception as exc:  # noqa: BLE001 - do not echo arbitrary refusal text
            return f"worker refuses connectorSecrets pool selection ({exception_type(exc)})"
        if pool not in pools:
            return f"selected SandboxWarmPool {pool} is absent"
        template = pools[pool]
        if template not in templates:
            return f"selected SandboxTemplate {template} is absent"
        for name in names:
            if name not in templates[template]:
                return f"SandboxTemplate {template} has no runner secretKeyRef for {name}"
        return None

    lines = []
    for binding in bindings:
        reason = reason_for(binding)
        lines.append(
            {
                "agent_readiness": "binding",
                **{key: binding[key] for key in ("agent", "kind", "address", "identity")},
                "ready": int(reason is None),
                "reason": reason or "no configuration obstacle observed in this snapshot",
            }
        )
    lines.append(
        {
            "agent_readiness": "total",
            "bindings": len(bindings),
            "not_ready": sum(line["ready"] == 0 for line in lines),
            "agents": len({binding["agent"] for binding in bindings}),
        }
    )
    return lines


class StructuredParser(argparse.ArgumentParser):
    """@spec STARTABILITY-6."""

    def error(self, message: str) -> Never:
        """@spec STARTABILITY-6; argparse messages can include untrusted argument values."""
        raise ValueError("invalid arguments")


def arguments(argv: list[str] | None) -> tuple[argparse.Namespace, str]:
    """@spec STARTABILITY-5 STARTABILITY-6."""
    parser = StructuredParser(description=__doc__)
    for name in (
        "worker-namespace",
        "worker-deployment",
        "worker-container",
        "dispatcher-namespace",
        "dispatcher-deployment",
        "dispatcher-container",
        "sandbox-namespace",
        "runner-container",
        "check-image",
    ):
        parser.add_argument("--" + name, required=True)
    args = parser.parse_args(argv)
    dsn = os.environ.get("DATABASE_URL", "").strip()
    if not all(value.strip() for value in vars(args).values()) or not dsn:
        raise ValueError("missing configuration")
    if dsn.startswith("postgresql://"):
        dsn = dsn.replace("postgresql://", "postgresql+asyncpg://", 1)
    if not dsn.startswith("postgresql+asyncpg://"):
        raise ValueError("unsupported database configuration")
    return args, dsn


def gather(args: argparse.Namespace, dsn: str) -> list[dict[str, Any]]:
    """@spec STARTABILITY-1 through STARTABILITY-6."""
    import sqlalchemy as sa
    from aci_protocol import BootEnv
    from curie_worker.sandbox.types import claim_warm_pool
    from kubernetes import client, config
    from plugin_format import is_reserved_boot_env_name
    from sqlalchemy.ext.asyncio import create_async_engine

    config.load_incluster_config()
    apps, core, custom = client.AppsV1Api(), client.CoreV1Api(), client.CustomObjectsApi()
    cache: dict[tuple[str, str, str], Any] = {}

    def container(namespace: str, deployment: str, name: str) -> Any:
        """@spec STARTABILITY-5."""
        spec = apps.read_namespaced_deployment(deployment, namespace).spec.template.spec
        matches = [c for c in spec.containers if c.name == name]
        if len(matches) != 1:
            raise ValueError("selected container is absent or ambiguous")
        return matches[0]

    worker = container(args.worker_namespace, args.worker_deployment, args.worker_container)
    if not worker.image or worker.image != args.check_image:
        raise ValueError("observer image does not match worker image")
    dispatcher = container(
        args.dispatcher_namespace, args.dispatcher_deployment, args.dispatcher_container
    )
    if worker.env_from or dispatcher.env_from:
        raise ValueError("inherited environment configuration is unsupported")

    def load(kind: str, namespace: str, name: str) -> Mapping[str, str | bytes]:
        """@spec STARTABILITY-4 STARTABILITY-5."""
        key = kind, namespace, name
        if key not in cache:
            try:
                if kind == "secret":
                    data = core.read_namespaced_secret(name, namespace).data or {}
                    cache[key] = {k: base64.b64decode(v, validate=True) for k, v in data.items()}
                else:
                    cache[key] = core.read_namespaced_config_map(name, namespace).data or {}
            except Exception as exc:  # noqa: BLE001 - cache failed referenced reads too
                cache[key] = exc
        result = cache[key]
        if isinstance(result, Exception):
            raise result
        return cast(Mapping[str, str | bytes], result)

    def entry(item: Any) -> dict[str, Any]:
        """@spec STARTABILITY-5."""
        result = {"name": item.name, "value": item.value}
        refs = item.value_from
        if refs is not None:
            for kind in ("secret", "config_map"):
                ref = getattr(refs, kind + "_key_ref", None)
                if ref is not None:
                    result[kind] = {"name": ref.name, "key": ref.key}
                    return result
            raise ValueError("unsupported environment reference")
        return result

    def resolve(item: dict[str, Any], namespace: str) -> str | None:
        """@spec STARTABILITY-3 STARTABILITY-5."""
        for kind in ("secret", "config_map"):
            if item.get(kind):
                ref = item[kind]
                value = load(kind, namespace, ref["name"])[ref["key"]]
                return value.decode("utf-8") if isinstance(value, bytes) else value
        return cast(str | None, item.get("value"))

    worker_env = {
        e.name: resolve(entry(e), args.worker_namespace)
        for e in worker.env or []
        if e.name in WORKER_VARIABLES
    }
    declaration = [
        {"name": e.name, "value": resolve(entry(e), args.dispatcher_namespace)}
        for e in dispatcher.env or []
        if e.name == "CURIE_SLACK_IDENTITIES"
    ]
    declared = identity_lanes(declaration)
    names = {name for pair in declared.values() for name in pair}
    lanes = credential_states(
        [entry(e) for e in dispatcher.env or [] if e.name in names],
        lambda name: load("secret", args.dispatcher_namespace, name),
        names=names,
        load_config_map=lambda name: load("config_map", args.dispatcher_namespace, name),
    )

    def items(plural: str) -> list[dict[str, Any]]:
        """@spec STARTABILITY-5."""
        return cast(
            list[dict[str, Any]],
            custom.list_namespaced_custom_object(
                "extensions.agents.x-k8s.io", "v1beta1", args.sandbox_namespace, plural
            )["items"],
        )

    pools = {
        p["metadata"]["name"]: p["spec"]["sandboxTemplateRef"]["name"]
        for p in items("sandboxwarmpools")
    }
    templates = {}
    for template in items("sandboxtemplates"):
        containers = template["spec"]["podTemplate"]["spec"]["containers"]
        templates[template["metadata"]["name"]] = {
            e["name"]
            for c in containers
            if c["name"] == args.runner_container
            for e in c.get("env") or []
            if (e.get("valueFrom") or {}).get("secretKeyRef")
        }

    async def bindings() -> list[Mapping[str, Any]]:
        """@spec STARTABILITY-1 STARTABILITY-5."""
        engine = create_async_engine(dsn)
        try:
            async with engine.connect() as conn:
                rows = (await conn.execute(sa.text(BINDINGS_SQL))).mappings().all()
                return [dict(row) for row in rows]
        finally:
            await engine.dispose()

    return judge(
        asyncio.run(bindings()),
        worker_env,
        lanes,
        pools,
        templates,
        identity_lanes=declared,
        claim=claim_warm_pool,
        reserved=is_reserved_boot_env_name,
        marker=BootEnv.env_key("connector_secret_keys"),
    )


def main(argv: list[str] | None = None) -> int:
    """@spec STARTABILITY-5 STARTABILITY-6."""
    try:
        args, dsn = arguments(argv)
        lines = gather(args, dsn)
        output = "\n".join(json.dumps(line) for line in lines)
    except Exception as exc:  # noqa: BLE001 - never print raw external or argument exceptions
        print(
            json.dumps(
                {
                    "agent_readiness": "error",
                    "reason": f"startability collection failed ({exception_type(exc)})",
                }
            )
        )
        return 1
    print(output)
    return 0


if __name__ == "__main__":
    sys.exit(main())
