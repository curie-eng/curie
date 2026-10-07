"""@spec STARTABILITY-7; subprocess adapters replace external reads only."""

from __future__ import annotations

import base64
import json
import os
from pathlib import Path
from types import SimpleNamespace
from typing import Any

if "STARTABILITY_TEST_SNAPSHOT" in os.environ:
    from kubernetes import client, config
    from sqlalchemy.ext import asyncio as sqlalchemy_asyncio

    DATA = json.loads(Path(os.environ["STARTABILITY_TEST_SNAPSHOT"]).read_text())
    TRACE = Path(os.environ["STARTABILITY_TEST_TRACE"])
    SENTINEL = "EXAMPLE-CREDENTIAL-MUST-NOT-APPEAR"

    def record(operation: str, args: list[str] | None = None, **extra: Any) -> None:
        """@spec STARTABILITY-7."""
        with TRACE.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps({"operation": operation, "args": args or [], **extra}) + "\n")

    def obj(value: Any) -> Any:
        """@spec STARTABILITY-7; emulate the Kubernetes client's attribute objects."""
        if isinstance(value, dict):
            return SimpleNamespace(**{key: obj(item) for key, item in value.items()})
        if isinstance(value, list):
            return [obj(item) for item in value]
        return value

    def env_entry(entry: dict) -> SimpleNamespace:
        """@spec STARTABILITY-7."""
        ref = None
        if "secret" in entry:
            ref = SimpleNamespace(secret_key_ref=obj(entry["secret"]), config_map_key_ref=None)
        elif "config_map" in entry:
            ref = SimpleNamespace(secret_key_ref=None, config_map_key_ref=obj(entry["config_map"]))
        return SimpleNamespace(name=entry["name"], value=entry.get("value"), value_from=ref)

    class Apps:
        """@spec STARTABILITY-5 STARTABILITY-7."""

        def read_namespaced_deployment(self, name: str, namespace: str, **_: Any) -> Any:
            record("deployment", [name, namespace])
            if DATA.get("deployment_error"):
                raise PermissionError("Kubernetes body " + SENTINEL)
            if (name, namespace) == ("acme-worker", "acme-workers"):
                role, container_name = "worker", "worker"
            elif (name, namespace) == ("acme-dispatcher", "acme-dispatchers"):
                role, container_name = "dispatcher", "dispatcher"
            else:
                raise AssertionError("observer read an unconfigured Deployment")
            container = SimpleNamespace(
                name=container_name,
                image=DATA["worker_image"],
                env=[env_entry(entry) for entry in DATA[role + "_env"]],
                env_from=[obj(entry) for entry in DATA.get(role + "_env_from", [])],
            )
            return SimpleNamespace(
                spec=SimpleNamespace(
                    template=SimpleNamespace(
                        spec=SimpleNamespace(containers=[container]),
                    )
                )
            )

    class Core:
        """@spec STARTABILITY-4 STARTABILITY-5 STARTABILITY-7."""

        def read_namespaced_secret(self, name: str, namespace: str, **_: Any) -> Any:
            record("secret", [name, namespace])
            assert namespace in {"acme-dispatchers", "acme-workers"}, "Secret namespace was not scoped"
            if DATA.get("secret_error") == name:
                raise PermissionError("credential response " + SENTINEL)
            secrets = (
                DATA["secrets"]
                if namespace == "acme-dispatchers"
                else DATA.get("worker_secrets", {})
            )
            return SimpleNamespace(
                data={
                    key: base64.b64encode(value.encode()).decode()
                    for key, value in secrets[name].items()
                }
            )

        def read_namespaced_config_map(self, name: str, namespace: str, **_: Any) -> Any:
            record("config_map", [name, namespace])
            assert namespace == "acme-workers", "ConfigMap namespace was not scoped"
            return SimpleNamespace(data=DATA["config_maps"][name])

    class Custom:
        """@spec STARTABILITY-5 STARTABILITY-7."""

        def list_namespaced_custom_object(
            self,
            group: str,
            version: str,
            namespace: str,
            plural: str,
            **_: Any,
        ) -> dict:
            record("custom", [group, version, namespace, plural])
            assert (group, version, namespace) == (
                "extensions.agents.x-k8s.io",
                "v1beta1",
                "acme-sandboxes",
            ), "sandbox resource scope was not configured"
            if DATA.get("custom_error"):
                raise PermissionError("Kubernetes response body " + SENTINEL)
            return {
                "items": DATA[
                    {"sandboxwarmpools": "pools", "sandboxtemplates": "templates"}[plural]
                ]
            }

    class Connection:
        """@spec STARTABILITY-1 STARTABILITY-7."""

        async def __aenter__(self) -> Any:
            return self

        async def __aexit__(self, *_: Any) -> None:
            pass

        async def execute(self, statement: Any, *_: Any, **__: Any) -> Any:
            record("database", sql=str(statement), asyncpg=ASYNC_DRIVER)
            if DATA.get("database_error"):
                raise RuntimeError("SQLAlchemy password=" + SENTINEL + " @database.example.com")
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: DATA["bindings"]))

    class Engine:
        """@spec STARTABILITY-5 STARTABILITY-7."""

        def connect(self) -> Connection:
            return Connection()

        async def dispose(self) -> None:
            record("database_dispose")

    def create_engine(dsn: str, **_: Any) -> Engine:
        """@spec STARTABILITY-5 STARTABILITY-7."""
        global ASYNC_DRIVER
        ASYNC_DRIVER = dsn.startswith("postgresql+asyncpg://")
        return Engine()

    config.load_incluster_config = lambda **_: None
    client.AppsV1Api = Apps
    client.CoreV1Api = Core
    client.CustomObjectsApi = Custom
    sqlalchemy_asyncio.create_async_engine = create_engine
