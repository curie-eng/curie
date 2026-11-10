"""Local fix pin for #3623 (ADR-0188): the sandbox memory credential is scoped.

The #3623 probe ran inside a live sandbox and found three holes in the state API:
editing ``CURIE_CHANNEL_MEMORY_REF`` reached another channel's memory, a raw PUT
stored any ``author``, and with memory writes off the sandbox could still write.
This re-runs that probe against a real API and real Postgres on a private
Compose stack built from this checkout, with the exact credentials a sandbox
holds: the boot env the real worker ``BindingResolver.boot_env`` renders, and the
per-turn credential its ``turn_memory_token`` mints. Nothing is mocked; only the
sandbox itself is stood in for by the HTTP calls it would make.

The stack follows ``test_connector_deploy_approval_routes.py``: a source-built
API image (or ``CURIE_LOCAL_MEMORY_SCOPE_API_IMAGE``), a uniquely named Compose
project on kernel-picked loopback ports, and a teardown that proves nothing it
owned survives. It never touches another stack.
"""

from __future__ import annotations

import asyncio
import json
import os
import pathlib
import socket
import subprocess
import time
import urllib.error
import urllib.request
import uuid
from base64 import urlsafe_b64decode
from collections.abc import Iterator
from dataclasses import dataclass
from urllib.parse import quote

import pytest

REPO = pathlib.Path(__file__).parents[3]
API_KEY = "curie-dev-key"
ALICE = "U0ALICE001"
FACT = "fact-" + "0123456789abcdef" * 2
OTHER_FACT = "fact-" + "fedcba9876543210" * 2


def _run(
    argv: list[str],
    *,
    env: dict[str, str] | None = None,
    timeout: int = 300,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        argv, cwd=REPO, env=env, text=True, capture_output=True, timeout=timeout, check=False
    )


def _require(result: subprocess.CompletedProcess[str], purpose: str) -> str:
    if result.returncode:
        raise RuntimeError(
            f"{purpose} failed with {result.returncode}\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
    return result.stdout


def _kernel_picked_ports(count: int) -> list[int]:
    probes = [socket.socket(socket.AF_INET, socket.SOCK_STREAM) for _ in range(count)]
    try:
        for probe in probes:
            probe.bind(("127.0.0.1", 0))
        return [probe.getsockname()[1] for probe in probes]
    finally:
        for probe in probes:
            probe.close()


@dataclass
class Response:
    status: int
    body: object


def _http(url: str, method: str = "GET", payload: object = None, *, key: str = API_KEY) -> Response:
    data = None if payload is None else json.dumps(payload).encode()
    request = urllib.request.Request(
        url,
        data=data,
        method=method,
        headers={"Content-Type": "application/json", "X-API-Key": key},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
            status = response.status
    except urllib.error.HTTPError as error:
        raw = error.read()
        status = error.code
    try:
        body: object = json.loads(raw) if raw else None
    except ValueError:
        body = raw.decode(errors="replace")
    return Response(status, body)


@pytest.fixture(scope="module")
def api_url(tmp_path_factory: pytest.TempPathFactory) -> Iterator[str]:
    """A private API + Postgres stack built from this checkout."""

    root = tmp_path_factory.mktemp("memory-scope-stack")
    suffix = uuid.uuid4().hex[:10]
    project = f"curie-3623-{suffix}"
    supplied = os.environ.get("CURIE_LOCAL_MEMORY_SCOPE_API_IMAGE")
    api_image = supplied or f"curie-3623-api:{suffix}"
    ports = dict(
        zip(("postgres", "valkey", "rustfs", "curie-api"), _kernel_picked_ports(4), strict=True)
    )
    override = root / "compose.override.yaml"
    override.write_text(
        f"""services:
  postgres:
    ports: !override ["127.0.0.1:{ports["postgres"]}:5432"]
  valkey:
    ports: !override ["127.0.0.1:{ports["valkey"]}:6379"]
  rustfs:
    ports: !override ["127.0.0.1:{ports["rustfs"]}:9000", "127.0.0.1::9001"]
  curie-migrate:
    image: {api_image}
    pull_policy: never
  curie-api:
    image: {api_image}
    pull_policy: never
    environment:
      API_KEY: {API_KEY}
      GITHUB_REVIEW_INGRESS_ENABLED: "false"
      GITHUB_APP_ID: ""
      GITHUB_APP_PRIVATE_KEY: ""
      GITHUB_WEBHOOK_SECRET: dev-webhook-secret
      SLACK_BOT_TOKEN: ""
      OTEL_EXPORTER_OTLP_ENDPOINT: ""
      OTEL_EXPORTER_OTLP_PROTOCOL: ""
    ports: !override ["127.0.0.1:{ports["curie-api"]}:8000"]
networks:
  curie_runner:
    name: "{project}_runner"
"""
    )
    env = os.environ.copy()
    env.update(
        {
            "COMPOSE_PROJECT_NAME": project,
            "CURIE_LOCAL_API_KEY": API_KEY,
            "OTEL_EXPORTER_OTLP_ENDPOINT": "",
        }
    )
    compose = [
        "docker",
        "compose",
        "-p",
        project,
        "-f",
        str(REPO / "compose.dev.yaml"),
        "-f",
        str(override),
        "--profile",
        "core",
    ]
    built = False
    errors: list[str] = []
    try:
        _require(_run(["docker", "info"]), "Docker prerequisite")
        if supplied:
            _require(_run(["docker", "image", "inspect", api_image]), "finding the API image")
        else:
            # Always this checkout's API: an unrelated published image cannot
            # make the fix pin green.
            _require(
                _run(
                    ["docker", "build", "-f", "apps/api/Dockerfile", "-t", api_image, "."],
                    timeout=1200,
                ),
                "building the source API image",
            )
            built = True
        _require(
            _run(
                compose
                + [
                    "up",
                    "-d",
                    "--wait",
                    "--wait-timeout",
                    "240",
                    "postgres",
                    "valkey",
                    "rustfs-perms",
                    "rustfs",
                    "rustfs-init",
                    "curie-migrate",
                    "curie-api",
                ],
                env=env,
                timeout=300,
            ),
            "starting the private API stack",
        )
        url = f"http://127.0.0.1:{ports['curie-api']}"
        ready = _http(f"{url}/agents")
        if ready.status != 200:
            raise RuntimeError(f"the private API is not serving: {ready}")
        yield url
    finally:
        down = _run(compose + ["down", "-v", "--remove-orphans"], env=env, timeout=180)
        if down.returncode:
            errors.append(f"Compose teardown failed: {down.stdout}\n{down.stderr}")
        survivors = _run(
            ["docker", "ps", "-aq", "--filter", f"label=com.docker.compose.project={project}"]
        ).stdout.split()
        volumes = _run(
            [
                "docker",
                "volume",
                "ls",
                "-q",
                "--filter",
                f"label=com.docker.compose.project={project}",
            ]
        ).stdout.split()
        if survivors or volumes:
            errors.append(f"owned resources survived: {survivors} {volumes}")
        if built:
            removed = _run(["docker", "image", "rm", "-f", api_image])
            if removed.returncode:
                errors.append(f"removing {api_image} failed: {removed.stderr}")
        if errors:
            raise RuntimeError("owned stack cleanup failed: " + "; ".join(errors))


@dataclass
class Sandbox:
    """What a sandbox on channel A holds, minted by the real worker."""

    agent_id: str
    channel_a: str
    channel_b: str
    env: dict[str, str]
    resolver: object
    resolved: object

    def turn_token(self, *, sender: str = ALICE, turn: str | None = None) -> str | None:
        return self.resolver.turn_memory_token(  # type: ignore[attr-defined]
            self.resolved,
            kind="slack",
            address=self.channel_a,
            thread_key=f"slack:{self.channel_a}:1700000000.000001",
            sender=sender,
            turn=turn or f"evt-{uuid.uuid4().hex}",
            ttl_s=300.0,
        )


def _sandbox(api_url: str, *, memory_writes: bool) -> Sandbox:
    from curie_worker.binding import BindingResolver, ResolvedDeployment
    from curie_worker.config import WorkerConfig

    # One agent per route: each test binds its own pair of channels.
    channel_a = f"C0A{uuid.uuid4().hex[:8].upper()}"
    channel_b = f"C0B{uuid.uuid4().hex[:8].upper()}"
    created = _http(
        f"{api_url}/agents",
        "POST",
        {
            "name": f"memory-scope-{uuid.uuid4().hex[:8]}",
            "channel": {"kind": "slack", "address": channel_a},
        },
    )
    assert created.status == 201, created
    agent_id = created.body["id"]  # type: ignore[index]
    second = _http(
        f"{api_url}/agents/{agent_id}/channels",
        "POST",
        {"kind": "slack", "address": channel_b},
    )
    assert second.status == 201, second

    config = WorkerConfig(api_key=API_KEY, api_base_url=api_url, runner_api_base_url=api_url)
    resolver = BindingResolver.__new__(BindingResolver)
    resolver._config = config  # type: ignore[attr-defined]
    resolved = ResolvedDeployment(
        agent_id=uuid.UUID(agent_id),
        agent_name="memory-scope",
        version_id=uuid.uuid4(),
        version_label="v1",
        bundle_ref="bundles/x.zip",
        max_usd_per_day=None,
        max_output_tokens_per_run=None,
        memory_writes=memory_writes,
    )
    env = resolver.boot_env(
        resolved, f"slack:{channel_a}:1700000000.000001", kind="slack", address=channel_a
    )
    return Sandbox(agent_id, channel_a, channel_b, env, resolver, resolved)


def _other_channel(sandbox: Sandbox) -> str:
    # Exactly the probe's edit: swap the channel in CURIE_CHANNEL_MEMORY_REF.
    ref = sandbox.env["CURIE_CHANNEL_MEMORY_REF"]
    own, other = f"/bindings/slack/{sandbox.channel_a}/", f"/bindings/slack/{sandbox.channel_b}/"
    assert f"{own}memory" in ref, ref
    return ref.replace(own, other)


def _fact(statement: str, author: str) -> dict[str, str]:
    return {
        "statement": statement,
        "author": author,
        "stated_at": "2026-10-01T00:00:00+00:00",
        "session_id": "sess-probe",
    }


def test_channel_a_sandbox_gets_403_on_channel_b(api_url: str) -> None:
    sandbox = _sandbox(api_url, memory_writes=True)
    own_ref = sandbox.env["CURIE_CHANNEL_MEMORY_REF"]
    other_ref = _other_channel(sandbox)
    seeded = _http(f"{other_ref}/{FACT}", "PUT", {"value": _fact("a DM secret", "U0BOB0001")})
    assert seeded.status == 200, seeded

    # The boot credential first: the #3623 probe read a DM's memory with it.
    boot_token = sandbox.env["CURIE_MEMORY_TOKEN"]
    got = _http(f"{other_ref}/{FACT}", key=boot_token)
    assert got.status == 403, got
    assert "a DM secret" not in json.dumps(got.body)
    assert _http(other_ref, key=boot_token).status == 403

    turn_token = sandbox.turn_token()
    assert turn_token, "the worker minted no per-turn memory credential"
    got = _http(f"{other_ref}/{FACT}", key=turn_token)
    assert got.status == 403, got
    assert "a DM secret" not in json.dumps(got.body)
    assert _http(other_ref, key=turn_token).status == 403
    planted = _http(
        f"{other_ref}/{OTHER_FACT}", "PUT", {"value": _fact("x", ALICE)}, key=turn_token
    )
    assert planted.status == 403, planted
    removed = _http(f"{other_ref}/{FACT}", "DELETE", key=turn_token)
    assert removed.status == 403, removed

    still = _http(f"{other_ref}/{FACT}")
    assert still.status == 200, still
    assert still.body["value"]["statement"] == "a DM secret"  # type: ignore[index]
    assert _http(f"{other_ref}/{OTHER_FACT}").status == 404

    # Its own channel stays reachable with the same credentials.
    own = _http(f"{own_ref}/{FACT}", "PUT", {"value": _fact("ours", ALICE)}, key=turn_token)
    assert own.status == 200, own
    assert _http(f"{own_ref}/{FACT}", key=boot_token).status == 200


def test_forged_author_is_overwritten(api_url: str) -> None:
    sandbox = _sandbox(api_url, memory_writes=True)
    turn_token = sandbox.turn_token(sender=ALICE)
    assert turn_token, "the worker minted no per-turn memory credential"
    for ref in (sandbox.env["CURIE_CHANNEL_MEMORY_REF"], sandbox.env["CURIE_MEMORY_REF"]):
        url = f"{ref}/{FACT}"
        put = _http(url, "PUT", {"value": _fact("planted", "U0MALLORY1")}, key=turn_token)
        assert put.status == 200, put
        stored = _http(url)
        assert stored.status == 200, stored
        assert stored.body["value"]["author"] == ALICE, stored  # type: ignore[index]
        assert stored.body["value"]["statement"] == "planted"  # type: ignore[index]


def test_writes_off_put_returns_403(api_url: str) -> None:
    sandbox = _sandbox(api_url, memory_writes=False)
    guidance = f"{sandbox.env['CURIE_MEMORY_REF']}/guidance"
    seeded = _http(guidance, "PUT", {"value": {"text": "operator guidance"}})
    assert seeded.status == 200, seeded

    # Writes off: the boot credential the sandbox holds is read-only on memory
    # (the probe overwrote guidance and planted a fact with it)...
    boot_token = sandbox.env["CURIE_MEMORY_TOKEN"]
    for url, value in (
        (guidance, {"text": "sneaky"}),
        (f"{sandbox.env['CURIE_MEMORY_REF']}/{FACT}", _fact("planted", ALICE)),
        (f"{sandbox.env['CURIE_CHANNEL_MEMORY_REF']}/{FACT}", _fact("planted", ALICE)),
    ):
        put = _http(url, "PUT", {"value": value}, key=boot_token)
        assert put.status == 403, (url, put)
    assert _http(guidance, "DELETE", key=boot_token).status == 403

    kept = _http(guidance, key=boot_token)
    assert kept.status == 200, kept
    assert kept.body["value"] == {"text": "operator guidance"}  # type: ignore[index]
    assert _http(f"{sandbox.env['CURIE_MEMORY_REF']}/{FACT}").status == 404
    # ...and the worker mints no write credential at all.
    assert sandbox.turn_token() is None


def test_channel_a_sandbox_gets_403_on_channel_b_transcript(api_url: str) -> None:
    # #3767: the same probe on history. Editing the thread key in
    # CURIE_HISTORY_REF reached another channel's (or a DM's) conversation.
    from channel_protocol import scoped_conversation_id

    sandbox = _sandbox(api_url, memory_writes=True)
    own_key = scoped_conversation_id("slack", sandbox.channel_a, "1700000000.000001")
    other_key = scoped_conversation_id("slack", sandbox.channel_b, "1700000000.000001")
    own_ref = sandbox.env["CURIE_HISTORY_REF"]
    assert own_ref.endswith(f"/state/transcript/{quote(own_key, safe='')}"), own_ref
    other_ref = own_ref.replace(quote(own_key, safe=""), quote(other_key, safe=""))
    secret = [{"role": "user", "content": "a DM secret"}]
    seeded = _http(other_ref, "PUT", {"value": secret})
    assert seeded.status == 200, seeded

    history_token = sandbox.env["CURIE_HISTORY_TOKEN"]
    turn_token = sandbox.turn_token()
    assert turn_token, "the worker minted no per-turn memory credential"
    for key in (history_token, turn_token):
        got = _http(other_ref, key=key)
        assert got.status == 403, got
        assert "a DM secret" not in json.dumps(got.body)
        replaced = _http(other_ref, "PUT", {"value": [{"role": "user"}]}, key=key)
        assert replaced.status == 403, replaced
        appended = _http(f"{other_ref}/append", "POST", {"item": {"role": "user"}}, key=key)
        assert appended.status == 403, appended
        removed = _http(other_ref, "DELETE", key=key)
        assert removed.status == 403, removed
    listed = _http(own_ref.rsplit("/", 1)[0], key=history_token)
    assert listed.status == 200, listed
    assert other_key not in {row["key"] for row in listed.body}  # type: ignore[union-attr]

    still = _http(other_ref)
    assert still.status == 200, still
    assert still.body["value"] == secret  # type: ignore[index]

    # Its own thread keeps the history loader's whole cycle (replace, read,
    # append) with the boot credential the runner holds.
    history = [{"role": "user", "content": "ours"}]
    assert _http(own_ref, "PUT", {"value": history}, key=history_token).status == 200
    assert _http(own_ref, key=history_token).body["value"] == history  # type: ignore[index]
    reply = {"role": "assistant", "content": "hi"}
    appended = _http(f"{own_ref}/append", "POST", {"item": reply}, key=history_token)
    assert appended.status == 200, appended

    # A targetless cron's sandbox (no binding) keeps its own thread, and no
    # channel's.
    cron_key = scoped_conversation_id("@cron", sandbox.agent_id, f"cron-{uuid.uuid4().hex}")
    cron_env = sandbox.resolver.boot_env(sandbox.resolved, cron_key)  # type: ignore[attr-defined]
    cron_ref, cron_token = cron_env["CURIE_HISTORY_REF"], cron_env["CURIE_HISTORY_TOKEN"]
    assert _http(cron_ref, "PUT", {"value": history}, key=cron_token).status == 200
    assert _http(cron_ref, key=cron_token).status == 200
    assert _http(other_ref, key=cron_token).status == 403


def test_turn_token_is_refused_after_its_turn_is_closed(api_url: str) -> None:
    # #3776: a credential copied during a turn must stop writing once the
    # worker reports that turn closed, even though it has not expired yet.
    sandbox = _sandbox(api_url, memory_writes=True)
    turn = f"evt-{uuid.uuid4().hex}#1"
    turn_token = sandbox.turn_token(sender=ALICE, turn=turn)
    assert turn_token, "the worker minted no per-turn memory credential"
    ref = sandbox.env["CURIE_CHANNEL_MEMORY_REF"]
    during = _http(f"{ref}/{FACT}", "PUT", {"value": _fact("during", ALICE)}, key=turn_token)
    assert during.status == 200, during

    # The real worker call, with the worker token the compose API is given.
    asyncio.run(
        sandbox.resolver.close_turn_memory(  # type: ignore[attr-defined]
            uuid.UUID(sandbox.agent_id), turn
        )
    )

    after = _http(f"{ref}/{OTHER_FACT}", "PUT", {"value": _fact("after", ALICE)}, key=turn_token)
    assert after.status == 403, after
    assert "turn has ended" in json.dumps(after.body), after
    assert _http(f"{ref}/{FACT}", "DELETE", key=turn_token).status == 403
    assert _http(f"{ref}/{OTHER_FACT}").status == 404
    # Reads with it still work, and the next turn's credential still writes.
    read = _http(f"{ref}/{FACT}", key=turn_token)
    assert read.status == 200, read
    assert read.body["value"]["statement"] == "during"  # type: ignore[index]
    next_token = sandbox.turn_token(sender=ALICE)
    assert next_token
    nxt = _http(f"{ref}/{OTHER_FACT}", "PUT", {"value": _fact("next", ALICE)}, key=next_token)
    assert nxt.status == 200, nxt


def _jwt_claims(token: str) -> dict[str, object]:
    segment = token.split(".")[1]
    padded = segment + "=" * (-len(segment) % 4)
    claims: dict[str, object] = json.loads(urlsafe_b64decode(padded))
    return claims


def test_boot_env_token_is_refused_after_its_claim_is_released(api_url: str) -> None:
    """#3823: a finished turn's boot token dies at the deadline plus grace,
    and a released claim refuses it immediately while it is still unexpired.
    """

    from curie_worker.binding import BOOT_TOKEN_GRACE_SECONDS, boot_token_facts
    from curie_worker.sandbox_token import mint

    sandbox = _sandbox(api_url, memory_writes=False)
    before = int(time.time())
    env = sandbox.resolver.boot_env(  # type: ignore[attr-defined]
        sandbox.resolved,
        f"slack:{sandbox.channel_a}:1700000000.000009",
        kind="slack",
        address=sandbox.channel_a,
        token_ttl_s=90,
    )
    after = int(time.time())
    history = env["CURIE_HISTORY_TOKEN"]
    state = env["CURIE_STATE_TOKEN"]
    claims = _jwt_claims(history)
    lifetime = 90 + BOOT_TOKEN_GRACE_SECONDS
    assert before + lifetime <= claims["exp"] <= after + lifetime
    assert claims["exp"] == _jwt_claims(state)["exp"]
    memory = env["CURIE_MEMORY_REF"]
    assert _http(memory, key=history).status == 200
    notes = f"{api_url}/agents/{sandbox.agent_id}/state/notes"
    assert _http(notes, key=state).status == 200

    # Expiry alone, with no release report: a token already past exp is refused.
    expired = mint(
        API_KEY,
        agent=sandbox.agent_id,
        scope="state",
        exp=int(time.time()) - 5,
        claims={
            "binding": f"slack:{sandbox.channel_a}",
            "memory": "read",
            "cred": uuid.uuid4().hex,
        },
    )
    assert _http(memory, key=expired).status == 401

    agent, cred, _exp = boot_token_facts(history)
    assert agent and cred
    sandbox.resolver.release_boot_credential_sync(agent, cred)  # type: ignore[attr-defined]
    refused = _http(memory, key=history)
    assert refused.status == 403, refused
    assert "released" in json.dumps(refused.body)
    refused_app = _http(notes, key=state)
    assert refused_app.status == 403, refused_app

    fresh = sandbox.resolver.boot_env(  # type: ignore[attr-defined]
        sandbox.resolved,
        f"slack:{sandbox.channel_a}:1700000000.000010",
        kind="slack",
        address=sandbox.channel_a,
        token_ttl_s=90,
    )
    assert _http(memory, key=fresh["CURIE_HISTORY_TOKEN"]).status == 200
