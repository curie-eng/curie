"""Signed HTTP qualification on the disposable ADR 0274 worker install.

Run through ``curie dev test-env run -- uv run pytest -s <this file>``.
The sibling worker fixture owns a real kind install and verifies teardown.
This suite moves admission to HTTP, using an independent contract signer.
The separate hosted case requires a live provider and a connector-only SecretRef.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import time
import uuid

import pytest
import yaml
from test_webhook_wake_worker import FULLNAME, NAME, WakeStack

BODY = b"{}"
HOOK = "queue-ready"


def assert_no_key(value, key):
    if key in json.dumps(value):
        raise AssertionError("wake credential escaped its approved surface")


def signed_headers(key, agent, hook, delivery, timestamp, body=BODY):
    context = json.dumps([agent, hook], ensure_ascii=True, separators=(",", ":")).encode()
    material = (
        b"curie.hook.wake.v1\n"
        + f"{timestamp}.{delivery}.{len(context)}:".encode()
        + context
        + body
    )
    return {
        "X-Curie-Timestamp": str(timestamp),
        "X-Curie-Delivery-Id": delivery,
        "X-Curie-Signature-256": "sha256="
        + hmac.new(key.encode(), material, hashlib.sha256).hexdigest(),
    }


class Qualification(WakeStack):
    def __init__(self, root):
        super().__init__(root)
        self.cluster = "test-adr0274-wake-e2e"
        self.context = f"kind-{self.cluster}"
        self.receipts = {}
        self.namespace_owned = False

    def kube(self, *args, stdin=None):
        output = super().kube(*args, stdin=stdin)
        if args == ("create", "namespace", NAME):
            self.namespace_owned = True
        return output

    def close(self):
        try:
            if self.namespace_owned:
                self.kube("delete", "namespace", NAME, "--wait=false")
        finally:
            super().close()

    def scheduler_tick(self):
        self.kube(
            "exec",
            "-i",
            f"deployment/{FULLNAME}-worker",
            "--",
            "python3",
            "-",
            stdin="import asyncio,os\n"
            "from curie_worker.run import build\n"
            "from curie_worker.config import WorkerConfig\n"
            "from curie_worker.worker_lifecycle import WorkerResources\n"
            "async def tick():\n"
            " resources=WorkerResources()\n"
            " try:\n"
            "  runtime=build(WorkerConfig(),os.environ,resources=resources)\n"
            "  await runtime.cron_loop.one_pass()\n"
            " finally: await resources.aclose()\n"
            "asyncio.run(tick())\n",
        )

    def capture_failure(self):
        super().capture_failure()
        pods = json.loads(self.kube("get", "pods", "-o", "json"))["items"]
        for index, pod in enumerate(pods):
            for container in pod["spec"]["containers"]:
                role = container["name"]
                if role not in {"runner", "server", "caller-proxy"}:
                    continue
                try:
                    output = self.kube("logs", pod["metadata"]["name"], "-c", role, "--tail=300")
                except RuntimeError:
                    output = "Container logs were unavailable during failure capture."
                (self.root / f"failure-{role}-{index}.log").write_text(output)

    def api_python(self, code):
        return json.loads(
            self.kube("exec", "-i", f"deployment/{FULLNAME}-api", "--", "python3", "-", stdin=code)
        )

    def http(self, path, *, method="GET", body=None, headers=None, admin=False, copies=1):
        request = {"path": path, "method": method, "body": body, "headers": headers or {}}
        return self.api_python(
            "import concurrent.futures,json,urllib.request,urllib.error\n"
            "from curie_api.config import get_settings\n"
            f"spec=json.loads({json.dumps(json.dumps(request))})\n"
            + ("spec['headers']['X-Api-Key']=get_settings().api_key\n" if admin else "")
            + "def send(_):\n"
            " request=urllib.request.Request('http://127.0.0.1:8000'+spec['path'],"
            "data=None if spec['body'] is None else spec['body'].encode(),"
            "headers=spec['headers'],method=spec['method'])\n"
            " try: response=urllib.request.urlopen(request,timeout=30)\n"
            " except urllib.error.HTTPError as error: response=error\n"
            " with response:\n"
            "  return {'status':response.status,'body':json.loads(response.read()),"
            "'cache':response.headers.get('Cache-Control')}\n"
            f"with concurrent.futures.ThreadPoolExecutor(max_workers={copies}) as pool:\n"
            f" print(json.dumps(list(pool.map(send,range({copies})))))\n"
        )

    def key(self, agent="acme-wake-idle", hook=HOOK):
        result = self.http(f"/agents/{self.agents[agent]}/hooks/{hook}/wake-secret", admin=True)[0]
        assert result["status"] == 200
        assert result["cache"] == "no-store"
        assert set(result["body"]) == {"agent_id", "hook", "generation", "secret"}
        return result["body"]["secret"]

    def wake(
        self,
        agent="acme-wake-idle",
        *,
        key=None,
        hook=HOOK,
        delivery=None,
        timestamp=None,
        body=BODY,
        query="",
        copies=1,
    ):
        agent_id = self.agents[agent]
        headers = signed_headers(
            key or self.key(agent),
            agent_id,
            hook,
            delivery or uuid.uuid4().hex,
            int(time.time()) if timestamp is None else timestamp,
            body,
        )
        return self.http(
            f"/hooks/{agent_id}/{hook}/wake{query}",
            method="POST",
            body=body.decode(),
            headers=headers,
            copies=copies,
        )

    def snapshot(self):
        return {
            "runs": self.sql(
                "SELECT COALESCE(json_agg(r ORDER BY id),'[]'::json) FROM curie.hook_runs r"
            ),
            "stream": self.redis("XRANGE", "curie:runs", "-", "+"),
        }

    def assert_refused(self, status, reason, **kwargs):
        before = self.snapshot()
        answers = self.wake(**kwargs)
        assert all(
            item["status"] == status and item["body"] == {"detail": reason} for item in answers
        )
        assert self.snapshot() == before

    def queued(self, agent="acme-wake-idle", **kwargs):
        answer = self.wake(agent, **kwargs)[0]
        assert answer["status"] == 202, answer
        assert set(answer["body"]) == {"status", "run_id"}
        assert answer["body"]["status"] == "queued"
        return answer["body"]["run_id"]

    def add_agent(self, agent):
        bundle = self.root / agent
        (bundle / ".claude-plugin").mkdir(parents=True)
        (bundle / "skills/wake").mkdir(parents=True)
        (bundle / ".claude-plugin/plugin.json").write_text(
            json.dumps(
                {
                    "name": agent,
                    "version": "1.0.0",
                    "description": "Wake source controls",
                    "triggers": [
                        {
                            "type": "webhook",
                            "wake": True,
                            "name": HOOK,
                            "path": "inbound",
                            "prompt": "Read pending items and record the result.",
                        }
                    ],
                }
            )
        )
        (bundle / "skills/wake/SKILL.md").write_text(
            "---\nname: wake\ndescription: Read pending work.\n---\nRead pending work.\n"
        )
        self.deploy_extra(agent, bundle)

    def deploy_extra(self, agent, bundle):
        receipt = json.loads(
            self.run(
                self.cli,
                "cluster",
                "deploy",
                "--context",
                self.context,
                "--namespace",
                NAME,
                "--release",
                NAME,
                "--agent",
                agent,
                "--plugin-dir",
                str(bundle),
                "--slack-channel",
                f"C0EXAMPLE{len(self.agents) + 1}",
                "--label",
                "source-controls",
                "--json",
            )
        )
        self.agents[agent] = receipt["agent"]["id"]
        self.receipts[agent] = receipt

    def deploy(self, agent):
        super().deploy(agent)
        bundle = self.root / agent
        self.receipts[agent] = json.loads(
            self.run(
                self.cli,
                "cluster",
                "deploy",
                "--context",
                self.context,
                "--namespace",
                NAME,
                "--release",
                NAME,
                "--agent",
                agent,
                "--plugin-dir",
                str(bundle),
                "--label",
                "wake-qualification",
                "--json",
            )
        )


@pytest.fixture(scope="module", params=["cluster", "local"])
def qualification(request, tmp_path_factory):
    from webhook_wake_local import LocalQualification

    root = tmp_path_factory.mktemp("webhook-wake-qualification")
    runtime = Qualification(root) if request.param == "cluster" else LocalQualification(root)
    if request.param == "cluster":
        existing = runtime.run("kind", "get", "clusters").splitlines()
        if runtime.cluster in existing:
            raise RuntimeError("qualification cluster already exists; preserve its owner")
    try:
        runtime.start()
        yield runtime
    finally:
        runtime.close()


def test_acceptance_1(qualification):
    """Receipt secrecy is one part of item 1; connector boot needs its own proof."""
    stack = qualification
    key = stack.key()
    for receipt in stack.receipts.values():
        assert "wake_triggers" in receipt
        assert_no_key(receipt, key)
    assert stack.redis("XLEN", "curie:runs") == 0


def test_acceptance_2(qualification):
    stack = qualification
    stack.worker(False)
    key = stack.key()
    if stack.key() != key:
        raise AssertionError("wake-key retrieval was not reproducible")
    stack.assert_refused(401, "invalid_wake_signature", key="incorrect-key")
    stack.assert_refused(401, "invalid_wake_signature", key=key, agent="acme-wake-http")
    stack.assert_refused(401, "invalid_wake_signature", key=key, hook="other-hook")
    stack.assert_refused(401, "invalid_wake_signature", timestamp=int(time.time()) - 301)
    absent = str(uuid.uuid4())
    absent_key = stack.api_python(
        "import base64,hashlib,hmac,json\n"
        "from curie_api.config import get_settings\n"
        f"material='curie.hook.wake.key.v1:{absent}:{HOOK}:1'.encode()\n"
        "print(json.dumps(base64.urlsafe_b64encode(hmac.new("
        "get_settings().api_key.encode(),material,hashlib.sha256).digest()).decode().rstrip('=')))\n"
    )
    before = stack.snapshot()
    headers = signed_headers(absent_key, absent, HOOK, uuid.uuid4().hex, int(time.time()))
    unknown = stack.http(f"/hooks/{absent}/{HOOK}/wake", method="POST", body="{}", headers=headers)[
        0
    ]
    assert unknown["status"] == 401 and unknown["body"] == {"detail": "invalid_wake_signature"}
    assert stack.snapshot() == before
    stack.assert_refused(400, "wake_query_forbidden", query="?prompt=")
    for body in (b'{"prompt":"override"}', b'{"target":null}', b" {}", b"[]"):
        stack.assert_refused(422, "wake_body_invalid", body=body)
    stack.pause("acme-wake-idle", True)
    stack.assert_refused(409, "hook_paused")
    stack.pause("acme-wake-idle", False)
    agent = stack.agents["acme-wake-idle"]
    for method, suffix in (("GET", ""), ("POST", "/rotate")):
        route = f"/agents/{agent}/hooks/{HOOK}/wake-secret{suffix}"
        assert stack.http(route, method=method, admin=True, body="{}")[0]["status"] == 422
        assert stack.http(route + "?override=", method=method, admin=True)[0]["status"] == 400
        assert stack.http(route, method=method)[0]["status"] in (401, 403)
    run = stack.queued()
    rotated = stack.http(
        f"/agents/{agent}/hooks/{HOOK}/wake-secret/rotate", method="POST", admin=True
    )[0]
    assert rotated["status"] == 200 and rotated["cache"] == "no-store"
    assert set(rotated["body"]) == {"agent_id", "hook", "generation"}
    if stack.key() == key:
        raise AssertionError("rotation did not replace the current wake key")
    stack.assert_refused(401, "invalid_wake_signature", key=key)
    stack.worker(True)
    stack.settled(run, "ran")


@pytest.mark.parametrize(
    "guard", ["protected", "tombstone", "pending", "remediation", "partition", "binding"]
)
def test_source_controls(qualification, guard):
    stack = qualification
    agent = f"acme-wake-source-{guard}"
    stack.add_agent(agent)
    key = stack.key(agent)
    stack.worker(False)
    agent_id = stack.agents[agent]
    if guard in {"protected", "tombstone", "pending"}:
        target = {
            "mode": "ordinary",
            "tool_access": None,
            "runtime_id": None,
            "qualification_id": None,
            "bundle_digest": None,
        }
        if guard == "protected":
            target.update(
                mode="protected",
                tool_access="read-only",
                runtime_id="33333333-3333-4333-8333-333333333333",
                qualification_id="44444444-4444-4444-8444-444444444444",
                bundle_digest="a" * 64,
            )
        intent = stack.api_python(
            "import json\n"
            "from curie_protected_hooks.source_policy_records import target_intent_sha256\n"
            f"print(json.dumps(target_intent_sha256(json.loads({json.dumps(json.dumps(target))}))))\n"
        )
        operation = str(uuid.uuid4())
        status = "pending" if guard == "pending" else "committed"
        stack.sql(
            "INSERT INTO curie.hook_source_operations "
            "(agent_id,hook,operation_id,intent_sha256,status,generation) "
            f"VALUES('{agent_id}','{HOOK}','{operation}','{intent}','{status}',1)"
        )
        if guard != "pending":
            values = ["NULL" if value is None else "'" + value + "'" for value in target.values()]
            stack.sql(
                "INSERT INTO curie.hook_source_policies "
                "(agent_id,hook,operation_id,generation,mode,tool_access,runtime_id,"
                "qualification_id,bundle_digest,legacy_generation) "
                f"VALUES('{agent_id}','{HOOK}','{operation}',1,{','.join(values)},0)"
            )
    elif guard == "remediation":
        stack.sql(
            "INSERT INTO curie.remediation_policies "
            "(agent_id,hook,generation,operation_id,armed,active) "
            f"VALUES('{agent_id}','{HOOK}',1,'{uuid.uuid4()}',false,false)"
        )
    else:
        column, value = (
            ("hook_partitions", {"pointer": "/item/id"})
            if guard == "partition"
            else ("source_bindings", {"workload_pointer": "/workload", "map": {}})
        )
        stack.sql(
            f"UPDATE curie.agents SET {column}='{json.dumps({HOOK: value})}'::jsonb "
            f"WHERE id='{agent_id}'"
        )
    before = stack.snapshot()
    stack.assert_refused(409, "wake_source_not_ordinary", agent=agent, key=key)
    route = f"/agents/{agent_id}/hooks/{HOOK}/wake-secret"
    for path, method in ((route, "GET"), (route + "/rotate", "POST")):
        response = stack.http(path, method=method, admin=True)[0]
        assert response["status"] == 409
        assert response["body"] == {"detail": "wake_source_not_ordinary"}
    assert stack.snapshot() == before
    assert (
        stack.sql(f"SELECT count(*) FROM curie.hook_wake_keys WHERE agent_id='{agent_id}'") == "0"
    )


def test_acceptance_3(qualification):
    stack = qualification
    stack.worker(False)
    delivery = uuid.uuid4().hex
    timestamp = int(time.time()) + 120
    answers = stack.wake(delivery=delivery, timestamp=timestamp, copies=8)
    assert sorted(item["status"] for item in answers) == [200] * 7 + [202]
    run = next(item["body"]["run_id"] for item in answers if item["status"] == 202)
    before = stack.snapshot()
    assert stack.wake()[0]["body"] == {"status": "coalesced"}
    assert stack.snapshot() == before
    assert stack.wake(delivery=delivery, timestamp=timestamp)[0]["body"] == {"status": "coalesced"}
    assert stack.snapshot() == before
    stack.worker(True)
    stack.settled(run, "ran")
    stack.worker(False)
    before = stack.snapshot()
    assert stack.wake(delivery=delivery, timestamp=timestamp)[0]["body"] == {"status": "coalesced"}
    assert stack.snapshot() == before
    fresh = stack.queued()
    stack.worker(True)
    stack.settled(fresh, "ran")


def test_acceptance_4(qualification):
    """HTTP admission through real worker/sandbox; hosted MCP remains separate."""
    stack = qualification
    idle = stack.queued()
    stack.settled(idle, "ran")
    http_run = stack.queued("acme-wake-http")
    stack.settled(http_run, "ran")
    stack.worker(False)
    blocked = stack.queued()
    stack.pause("acme-wake-idle", True)
    stack.worker(True)
    stack.settled(blocked, "blocked", "hook_paused")
    stack.pause("acme-wake-idle", False)
    stack.scheduler_tick()
    assert stack.state(blocked)["outcome"] == "blocked"
    later = stack.queued()
    stack.settled(later, "ran")
    gated = stack.queued("acme-wake-gated")
    stack.settled(gated, "failed", "approval_gate_targetless")
    assert stack.sql("SELECT count(*) FROM curie.approvals") == "0"
    assert stack.slack_calls == []
    entries = stack.redis("XRANGE", "curie:runs", "-", "+")
    events = [json.loads(fields["payload"]) for _, fields in entries]
    assert all(
        event["reply_handle"] is None
        and event.get("author_principal") is None
        and event["author"] == "cron:queue-ready"
        and ":sweep:" not in event["event_id"]
        for event in events
    )


def test_acceptance_5(qualification):
    """Observe stored and operator provenance independently of the queue label."""
    stack = qualification
    run = stack.queued()
    stack.settled(run, "ran")
    rows = stack.sql(
        "SELECT COALESCE(json_agg(json_build_object('source',source,"
        "'lease_s',extract(epoch FROM lease_expires_at-started_at))), '[]'::json) "
        "FROM curie.hook_runs"
    )
    assert rows and all(row["source"] == "webhook" for row in rows)
    assert all(row["lease_s"] >= stack.claim_lease_s for row in rows)
    for _, fields in stack.redis("XRANGE", "curie:runs", "-", "+"):
        event = json.loads(fields["payload"])
        assert event["source"] == "cron" and event["hook_run"]["name"] == HOOK
    agent = stack.agents["acme-wake-idle"]
    run = stack.sql(
        "SELECT json_build_object('id',id) FROM curie.hook_runs "
        f"WHERE agent_id='{agent}' ORDER BY slot_utc DESC LIMIT 1"
    )["id"]
    route = f"/agents/{agent}/hooks/{HOOK}/runs/{run}"
    assert stack.http(route)[0]["status"] in (401, 403)
    read = stack.http(route, admin=True)[0]
    assert read["status"] == 200
    assert read["body"]["trigger"] == "webhook"
    assert_no_key(read, stack.key())


@pytest.mark.parametrize("qualification", ["cluster"], indirect=True)
def test_hosted_connector_session(qualification):
    """A live model reads and writes through the real hosted MCP connector."""
    stack = qualification
    credential_name = next(
        (
            name
            for name in ("CURIE_CREDENTIALS", "CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY")
            if os.environ.get(name)
        ),
        None,
    )
    if credential_name is None:
        raise RuntimeError("live connector qualification requires a provider credential")
    credential = os.environ[credential_name]
    agent = "acme-wake-idle"
    agent_id = stack.agents[agent]
    key = stack.key(agent)
    image = f"curie-wake-connector:{stack.tag}"
    stack.run(
        "docker",
        "build",
        "-f",
        "cli/tests/local/webhook_wake_connector.Dockerfile",
        "--build-arg",
        f"WORKER_IMAGE={stack.images['worker']}",
        "-t",
        image,
        ".",
        timeout=1800,
    )
    stack.images["connector"] = image
    stack.run("kind", "load", "docker-image", "--name", stack.cluster, image, timeout=300)
    for name, value in (("acme-wake-credential", key), ("acme-model-credential", credential)):
        stack.kube(
            "apply",
            "-f",
            "-",
            stdin=json.dumps(
                {
                    "apiVersion": "v1",
                    "kind": "Secret",
                    "metadata": {"name": name},
                    "stringData": {"key": value},
                }
            ),
        )
    extra = []
    for name in ("ANTHROPIC_BASE_URL",):
        if os.environ.get(name):
            extra.append({"name": name, "value": os.environ[name]})
    live = stack.root / "live.yaml"
    runner = {"fakeModel": False, "extraEnv": extra}
    if credential_name == "CURIE_CREDENTIALS":
        runner.update(
            credentialsExistingSecret="acme-model-credential",
            credentialsExistingSecretKey="key",
        )
    else:
        extra.append(
            {
                "name": credential_name,
                "valueFrom": {"secretKeyRef": {"name": "acme-model-credential", "key": "key"}},
            }
        )
    if os.environ.get("CURIE_MODEL"):
        runner["model"] = os.environ["CURIE_MODEL"]
    live.write_text(
        yaml.safe_dump(
            {
                "agentSandbox": {"runner": runner},
                "worker": {"extraEnv": extra},
            }
        )
    )
    from test_webhook_wake_worker import REPO

    stack.run(
        "helm",
        "upgrade",
        NAME,
        str(REPO / "charts/curie"),
        "--kube-context",
        stack.context,
        "--namespace",
        NAME,
        "--reuse-values",
        "-f",
        str(live),
        "--wait",
        "--timeout",
        "300s",
        timeout=360,
    )
    bundle = stack.root / agent
    manifest_path = bundle / ".claude-plugin/plugin.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["triggers"][0]["prompt"] = (
        "Call queue_read to read pending items. For each item call queue_complete with its id "
        "as item_id and the exact result receipt-ok. Use only those two connector tools."
    )
    manifest_path.write_text(json.dumps(manifest))
    (bundle / "connectors.yaml").write_text(
        yaml.safe_dump(
            {
                "connectors": {
                    "queue": {
                        "image": image,
                        "no_client_auth": "Caller identity is verified by the platform proxy.",
                        "scratch": {"path": "/tmp", "size_limit": "16Mi"},
                        "env": {
                            "WAKE_URL": f"http://{FULLNAME}-api:8000/hooks/{agent_id}/{HOOK}/wake"
                        },
                        "secrets": [
                            {
                                "name": "CURIE_HOOK_WAKE_KEY",
                                "from_secret": "acme-wake-credential",
                                "key": "key",
                            }
                        ],
                    }
                }
            }
        )
    )
    receipt = json.loads(
        stack.run(
            stack.cli,
            "cluster",
            "deploy",
            "--context",
            stack.context,
            "--namespace",
            NAME,
            "--release",
            NAME,
            "--agent",
            agent,
            "--plugin-dir",
            str(bundle),
            "--label",
            "live-queue",
            "--json",
        )
    )
    assert_no_key(receipt, key)
    deployments = json.loads(stack.kube("get", "deployments", "-o", "json"))["items"]
    connectors = [
        item["metadata"]["name"]
        for item in deployments
        if any(
            container["image"] == image
            for container in item["spec"]["template"]["spec"]["containers"]
        )
    ]
    assert len(connectors) == 1
    connector = connectors[0]
    stack.kube("rollout", "status", f"deployment/{connector}", "--timeout=180s")

    def control(path, value=None):
        return json.loads(
            stack.kube(
                "exec",
                "-i",
                f"deployment/{connector}",
                "-c",
                "server",
                "--",
                "python3",
                "-",
                stdin="import json,urllib.request\n"
                + f"data={None if value is None else json.dumps(json.dumps(value))}\n"
                + "request=urllib.request.Request('http://127.0.0.1:8001'"
                + f"+{path!r},data=None if data is None else data.encode(),"
                + "headers={'Content-Type':'application/json'})\n"
                + "print(urllib.request.urlopen(request,timeout=30).read().decode())\n",
            )
        )

    before = stack.snapshot()
    assert control("/poll", {}) == {"status": "empty"}
    assert stack.snapshot() == before
    control("/items", {"id": "item-1", "text": "Record the result receipt-ok."})
    polled = control("/poll", {})
    assert polled["http_status"] == 202
    run = polled["receipt"]["run_id"]
    stack.settled(run, "ran")
    observation = control("/observation")
    assert observation["items"] == [{"id": "item-1", "result": "receipt-ok"}]
    assert {call["tool"] for call in observation["calls"]} == {"read", "complete"}
    assert all(call["agent"] == agent for call in observation["calls"])
    assert all(not call["human_header"] and not call["key_header"] for call in observation["calls"])
    assert_no_key(stack.transcripts(agent), key)
    assert stack.slack_calls == []
