"""Serve one connector implementation and its public proxies without Kubernetes."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import logging
import os
import re
import signal
import ssl
import sys
from collections.abc import Mapping
from pathlib import Path
from typing import Annotated, Any, Literal, Self
from urllib.parse import quote

from aiohttp import web
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from . import caller
from .server import ProxyConfig, RouteConfig, _ValkeyGrantStore, make_app

CONFIG_ENV = "CURIE_CONNECTOR_SERVE_CONFIG"
PASSWORD_ENV = "CURIE_CONNECTOR_GRANT_STORE_PASSWORD"
Port = Annotated[int, Field(ge=1, le=65535)]
Name = Annotated[str, Field(pattern=r"^[a-z][a-z0-9_-]{0,62}$")]


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True, hide_input_in_errors=True)


class TLSConfig(_Model):
    cert_file: str
    key_file: str

    @field_validator("cert_file", "key_file")
    @classmethod
    def absolute_file(cls, value: str) -> str:
        if not Path(value).is_absolute():
            raise ValueError("TLS files must use absolute mounted paths")
        return value

    def context(self) -> ssl.SSLContext:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.minimum_version = ssl.TLSVersion.TLSv1_2
        context.load_cert_chain(self.cert_file, self.key_file)
        return context


class Listener(_Model):
    host: str
    port: Port
    tls: TLSConfig | None = None

    @field_validator("host")
    @classmethod
    def bind_address(cls, value: str) -> str:
        ipaddress.ip_address(value)
        return value


class Implementation(_Model):
    command: Annotated[list[str], Field(min_length=1)]
    port: Port = 8000
    upstream_host: Literal["localhost", "127.0.0.1", "::1"] = "127.0.0.1"

    @field_validator("command")
    @classmethod
    def nonempty_command(cls, value: list[str]) -> list[str]:
        if any(not part or "\0" in part for part in value):
            raise ValueError("implementation command arguments must be nonempty")
        return value


class Route(_Model):
    path: str
    methods: Annotated[list[Literal["POST", "PUT"]], Field(min_length=1)]
    max_body_bytes: Annotated[int, Field(ge=1, le=256 * 1024 * 1024)]

    @field_validator("path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        if not re.fullmatch(r"/(?:[a-z0-9_-]+/)*[a-z0-9_-]+", value) or value == "/mcp":
            raise ValueError("routes require a canonical lowercase path distinct from /mcp")
        return value

    def proxy_route(self) -> RouteConfig:
        return RouteConfig(self.path, frozenset(self.methods), self.max_body_bytes)


class Upload(_Model):
    listener: Listener
    upstream_port: Port = 8001
    path: str
    max_body_bytes: Annotated[int, Field(ge=1, le=256 * 1024 * 1024)]

    @field_validator("path")
    @classmethod
    def canonical_path(cls, value: str) -> str:
        return Route.canonical_path(value)


class GrantStore(_Model):
    host: str
    port: Port = 6379
    username: Name
    ca_file: str | None = None

    @field_validator("host")
    @classmethod
    def hostname(cls, value: str) -> str:
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.-]*", value):
            raise ValueError("grant store host must be a DNS name or IPv4 address")
        return value

    @field_validator("username")
    @classmethod
    def scoped_user(cls, value: str) -> str:
        if value == "default":
            raise ValueError("grant store requires a dedicated ACL user")
        return value

    @field_validator("ca_file")
    @classmethod
    def absolute_ca(cls, value: str | None) -> str | None:
        if value is not None and not Path(value).is_absolute():
            raise ValueError("grant store CA requires an absolute mounted path")
        return value


class HostConfig(_Model):
    version: Literal[1]
    connector: Name
    listener: Listener
    implementation: Implementation
    public_keys: Annotated[list[str], Field(min_length=1)]
    admits: Annotated[list[Name], Field(min_length=1)]
    gated_tools: list[str]
    routes: list[Route] = Field(default_factory=list)
    grant_store: GrantStore
    upload: Upload | None = None

    @field_validator("version", mode="before")
    @classmethod
    def integer_version(cls, value: Any) -> Any:
        if type(value) is not int:
            raise ValueError("version must be the integer 1")
        return value

    @field_validator("public_keys")
    @classmethod
    def verification_keys(cls, value: list[str]) -> list[str]:
        for text in value:
            caller.public_key(text)
        return value

    @field_validator("gated_tools")
    @classmethod
    def nonempty_patterns(cls, value: list[str]) -> list[str]:
        if any(not pattern.strip() for pattern in value):
            raise ValueError("gated patterns must be nonempty")
        return value

    @model_validator(mode="after")
    def distinct_boundaries(self) -> Self:
        public_ports = [self.listener.port]
        private_ports = [self.implementation.port]
        paths = [route.path for route in self.routes]
        if self.upload is not None:
            public_ports.append(self.upload.listener.port)
            private_ports.append(self.upload.upstream_port)
            paths.append(self.upload.path)
        if len(set(public_ports)) != len(public_ports) or set(public_ports) & set(private_ports):
            raise ValueError("public listeners must use distinct ports outside task loopback")
        if len(set(paths)) != len(paths):
            raise ValueError("route and upload paths must be distinct")
        return self

    def proxy_configs(self, env: Mapping[str, str]) -> tuple[ProxyConfig, ProxyConfig | None]:
        password = env.get(PASSWORD_ENV, "")
        if not password:
            raise ValueError("grant store password is required")
        store = self.grant_store
        url = (
            f"rediss://{quote(store.username, safe='')}:{quote(password, safe='')}"
            f"@{store.host}:{store.port}/0"
        )
        keys = tuple(caller.public_key(text) for text in self.public_keys)
        main = ProxyConfig(
            listen_port=self.listener.port,
            upstream_port=self.implementation.port,
            public_keys=keys,
            admits=frozenset(self.admits),
            connector=self.connector,
            upstream_host=self.implementation.upstream_host,
            gated_tools=tuple(self.gated_tools),
            routes=tuple(route.proxy_route() for route in self.routes),
            grant_store=_ValkeyGrantStore(
                url, ca_file=store.ca_file, key_prefix=f"connector-grant:{self.connector}:"
            ),
            upload_path=self.upload.path if self.upload is not None else None,
        )
        upload = None
        if self.upload is not None:
            upload = ProxyConfig(
                listen_port=self.upload.listener.port,
                upstream_port=self.upload.upstream_port,
                public_keys=keys,
                admits=frozenset(self.admits),
                connector=self.connector,
                upstream_host=self.implementation.upstream_host,
                routes=(
                    RouteConfig(self.upload.path, frozenset({"POST"}), self.upload.max_body_bytes),
                ),
                upload_only=True,
            )
        return main, upload


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate host configuration key")
        result[key] = value
    return result


def load_config(env: Mapping[str, str]) -> HostConfig:
    try:
        path = Path(env[CONFIG_ENV])
        if not path.is_absolute():
            raise ValueError
        document = json.loads(path.read_text(encoding="utf-8"), object_pairs_hook=_unique_object)
        return HostConfig.model_validate(document)
    except (KeyError, OSError, ValueError):
        raise ValueError(
            "invalid connector host configuration; check the mounted JSON file"
        ) from None


async def serve(config: HostConfig, env: Mapping[str, str]) -> int:
    main, upload = config.proxy_configs(env)
    listeners = [(config.listener, main)]
    if config.upload is not None and upload is not None:
        listeners.append((config.upload.listener, upload))
    contexts = [listener.tls.context() if listener.tls else None for listener, _ in listeners]
    stopped = asyncio.Event()
    stop_signal = signal.SIGTERM

    def stop_requested(sig: signal.Signals) -> None:
        nonlocal stop_signal
        stop_signal = sig
        stopped.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_requested, sig)
    child = await asyncio.create_subprocess_exec(
        *config.implementation.command, start_new_session=True
    )
    runners: list[web.AppRunner] = []
    child_exit = asyncio.create_task(child.wait())
    stop = asyncio.create_task(stopped.wait())
    result = 0
    try:
        for (listener, proxy), context in zip(listeners, contexts, strict=True):
            runner = web.AppRunner(make_app(proxy), handler_cancellation=True, shutdown_timeout=5)
            runners.append(runner)
            await runner.setup()
            await web.TCPSite(runner, listener.host, listener.port, ssl_context=context).start()
        done, _ = await asyncio.wait((child_exit, stop), return_when=asyncio.FIRST_COMPLETED)
        if child_exit in done:
            result = max(child_exit.result(), 1)
    finally:
        for runner in runners:
            await runner.cleanup()
        if child.returncode is None:
            os.killpg(child.pid, stop_signal)
            try:
                await asyncio.wait_for(asyncio.shield(child_exit), timeout=10)
            except TimeoutError:
                os.killpg(child.pid, signal.SIGKILL)
                await child_exit
                result = 1
        stop.cancel()
        await asyncio.gather(stop, return_exceptions=True)
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.remove_signal_handler(sig)
    return result


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s %(message)s")
    try:
        return asyncio.run(serve(load_config(os.environ), os.environ))
    except (OSError, ValueError):
        print(
            "connector host startup failed; check configuration and mounted TLS files",
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
