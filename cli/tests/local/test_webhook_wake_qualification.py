"""Signed HTTP qualification on the disposable ADR 0274 worker install.

Run through ``curie dev test-env run -- uv run pytest -o addopts= -s -rs <this file>``.
The sibling worker fixture owns a real kind install and verifies teardown.
This suite moves admission to HTTP, using an independent contract signer.
Hosted cases use a connector-only SecretRef. The live cases require
``WAKE_TEST_MODEL_CREDENTIAL`` and otherwise record an explicit acceptance gap.
The live key replaces the scripted placeholder in ``CURIE_CREDENTIALS``.
``WAKE_TEST_MODEL_BASE_URL`` (default Anthropic) selects the endpoint, and
``WAKE_TEST_MODEL_EGRESS_CIDRS`` declares runner egress for any other endpoint.
Local hosted sessions remain a gap because local/skill bring-up cannot deliver
operator-managed SecretRefs. The two transport cases wait about 150 seconds
per stack for real lease expiry on the database clock.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import os
import threading
import time
import urllib.parse
import uuid
from contextlib import contextmanager
from pathlib import Path

import pytest
import yaml
from test_webhook_wake_worker import FULLNAME, NAME, WakeStack, wait_until

BODY = b"{}"
HOOK = "queue-ready"
LIVE_PROVIDER_GAP = (
    "ADR 0274 live-provider connector/session gap: approved credential absent; "
    "Brian waived the live run on 2026-11-08 pending a test credential"
)
ANTHROPIC_API_HOST = "api.anthropic.com"
# Anthropic's published API range, the same entry the chart README gives for
# security.networkPolicy.allowedEgress on a direct Anthropic install.
ANTHROPIC_API_CIDR = "160.79.104.0/23"


def live_model_egress(base_url, declared_cidrs):
    """Runner egress entries for the live model endpoint.

    NetworkPolicy cannot target a host name, so the CIDRs are declared, never
    resolved. The default Anthropic endpoint uses its published range; any
    other endpoint must name its CIDRs in ``WAKE_TEST_MODEL_EGRESS_CIDRS``.
    """
    url = urllib.parse.urlsplit(base_url or f"https://{ANTHROPIC_API_HOST}")
    port = url.port or (443 if url.scheme == "https" else 80)
    cidrs = [cidr.strip() for cidr in declared_cidrs.split(",") if cidr.strip()]
    if not cidrs:
        if url.hostname != ANTHROPIC_API_HOST:
            raise RuntimeError(
                "set WAKE_TEST_MODEL_EGRESS_CIDRS to the CIDRs of WAKE_TEST_MODEL_BASE_URL; "
                "runner egress cannot target a host name"
            )
        cidrs = [ANTHROPIC_API_CIDR]
    return [{"cidr": cidr, "ports": [{"protocol": "TCP", "port": port}]} for cidr in cidrs]


def provider_tier_values(*, base_url, model, credential_secret, allowed_egress):
    """Helm values one provider tier layers over the install with ``--reuse-values``.

    The tier's credential replaces the install's scripted placeholder as the
    runner credential reference (``CURIE_CREDENTIALS``). A key carried only in
    ``ANTHROPIC_API_KEY`` loses to that placeholder once a base URL is set,
    because the runner reads the declared credential key first. Lists replace
    the install's lists, so ``allowed_egress`` must keep the install entries.
    """
    extra = [{"name": "CURIE_MODEL_BASE_URL", "value": base_url}] if base_url else []
    runner = {
        "fakeModel": False,
        "extraEnv": list(extra),
        "credentialsExistingSecret": credential_secret,
        "credentialsExistingSecretKey": "key",
    }
    if model:
        runner["model"] = model
    values = {"agentSandbox": {"runner": runner}, "worker": {"extraEnv": list(extra)}}
    if allowed_egress is not None:
        values["security"] = {"networkPolicy": {"allowedEgress": allowed_egress}}
    return values


class PrivateInput(str):
    def __repr__(self):
        return "<private subprocess input>"


def assert_no_key(value, key):
    __tracebackhide__ = True
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


def ordinary_headers(key, hook, delivery, timestamp, body=BODY):
    context = json.dumps([hook, None], separators=(",", ":")).encode()
    material = (
        b"curie.hook.delivery.v2\n"
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
        self.env["CURIE_CREDENTIALS"] = "not-needed"
        self.env["COMPOSE_DISABLE_ENV_FILE"] = "1"
        self.cluster = "test-adr0274-wake-e2e"
        self.context = f"kind-{self.cluster}"
        self.receipts = {}
        self.namespace_owned = False
        self.exports = []
        self.export_event = threading.Event()

    def model_response(self, request):
        tools = {item["name"] for item in request.get("tools", [])}
        read = next((name for name in tools if name.endswith("queue_read")), None)
        complete = next((name for name in tools if name.endswith("queue_complete")), None)
        if read is None or complete is None:
            return super().model_response(request)
        from test_webhook_wake_worker import message_to_sse

        def documents(value):
            if isinstance(value, dict):
                yield value
                for child in value.values():
                    yield from documents(child)
            elif isinstance(value, list):
                for child in value:
                    yield from documents(child)
            elif isinstance(value, str):
                try:
                    yield from documents(json.loads(value))
                except ValueError:
                    pass

        messages = request.get("messages", [])
        opening = 0
        for index, message in enumerate(messages):
            content = message.get("content", [])
            tool_result = isinstance(content, list) and any(
                isinstance(block, dict) and block.get("type") == "tool_result" for block in content
            )
            if message.get("role") == "user" and not tool_result:
                opening = index
        values = list(documents(messages[opening:]))
        completed = any(value.get("recorded") is True for value in values)
        items = next((value["items"] for value in reversed(values) if "items" in value), None)
        tool = None if completed or items == [] else read if items is None else complete
        arguments = {} if not items else {"item_id": items[0]["id"], "result": "receipt-ok"}
        return message_to_sse(
            {
                "id": f"msg_acme_{uuid.uuid4().hex}",
                "model": request.get("model", "claude-sonnet-5"),
                "content": [
                    {
                        "type": "tool_use",
                        "id": f"toolu_acme_{uuid.uuid4().hex}",
                        "name": tool,
                        "input": arguments,
                    }
                    if tool
                    else {"type": "text", "text": "done"}
                ],
                "stop_reason": "tool_use" if tool else "end_turn",
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }
        )

    def start(self):
        super().start()
        runtime = self
        parent = self.slack.RequestHandlerClass

        class Telemetry(parent):
            def do_POST(self):
                if not self.path.startswith("/otlp/"):
                    return super().do_POST()
                runtime.exports.append(
                    (self.path, self.rfile.read(int(self.headers.get("Content-Length", "0"))))
                )
                runtime.export_event.set()
                self.send_response(200)
                self.send_header("Content-Type", "application/x-protobuf")
                self.send_header("Content-Length", "0")
                self.end_headers()

        self.slack.RequestHandlerClass = Telemetry
        self.upgrade(
            {
                "otelCollector": {
                    "deploy": False,
                    "telemetryDisabled": False,
                    "endpoint": self.model_url.rsplit("/", 1)[0] + "/otlp",
                    "protocol": "http/protobuf",
                    "egress": [
                        {
                            "cidr": f"{os.environ['CURIE_LOCAL_STUB_HOST']}/32",
                            "ports": [{"protocol": "TCP", "port": self.slack.server_port}],
                        }
                    ],
                }
            }
        )

    def upgrade(self, values):
        from test_webhook_wake_worker import REPO

        current = json.loads(
            self.run(
                "helm",
                "get",
                "values",
                NAME,
                "--kube-context",
                self.context,
                "--namespace",
                NAME,
                "-o",
                "json",
            )
        )

        def update(target, changes):
            for key, value in changes.items():
                if isinstance(value, dict):
                    update(target.setdefault(key, {}), value)
                else:
                    target[key] = value

        update(current, values)
        path = self.root / "qualification-values.yaml"
        path.write_text(yaml.safe_dump(current))
        self.run(
            "helm",
            "upgrade",
            NAME,
            str(REPO / "charts/curie"),
            "--kube-context",
            self.context,
            "--namespace",
            NAME,
            "--reset-values",
            "-f",
            str(path),
            "--wait",
            "--timeout",
            "900s",
            timeout=960,
        )

    def telemetry_snapshot(self):
        from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
            ExportMetricsServiceRequest,
        )
        from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
            ExportTraceServiceRequest,
        )

        services, wakes = set(), []
        for path, raw in self.exports:
            if path.endswith("/traces"):
                request = ExportTraceServiceRequest.FromString(raw)
                for resource in request.resource_spans:
                    services.update(
                        attribute.value.string_value
                        for attribute in resource.resource.attributes
                        if attribute.key == "service.name"
                    )
            if path.endswith("/metrics"):
                request = ExportMetricsServiceRequest.FromString(raw)
                for resource in request.resource_metrics:
                    for scope in resource.scope_metrics:
                        for metric in scope.metrics:
                            if metric.name != "curie.schedule.fire":
                                continue
                            for point in metric.sum.data_points:
                                attributes = {
                                    item.key: item.value.string_value for item in point.attributes
                                }
                                if attributes.get("trigger") == "webhook":
                                    wakes.append(attributes)
        return {"services": services, "wakes": wakes}

    def wait_telemetry(self):
        deadline = time.monotonic() + 75
        while True:
            self.export_event.clear()
            snapshot = self.telemetry_snapshot()
            services_present = {"curie-api", "curie-worker", "curie-runner"} <= snapshot["services"]
            if services_present and snapshot["wakes"]:
                return snapshot
            remaining = deadline - time.monotonic()
            if remaining <= 0 or not self.export_event.wait(remaining):
                raise AssertionError("missing actual API/worker/runner traces or webhook metrics")

    def restart_api(self):
        self.kube("rollout", "restart", f"deployment/{FULLNAME}-api")
        self.kube("rollout", "status", f"deployment/{FULLNAME}-api", "--timeout=600s")

    def configure_lease(self, budget, lease):
        self.upgrade(
            {
                "worker": {
                    "deliveryBudgetSeconds": budget,
                    "runnerTotalTimeoutSeconds": budget,
                    "hookClaimLeaseSeconds": lease,
                }
            }
        )
        self.claim_lease_s = lease if lease is not None else budget
        api = self.api_python(
            "import json;from curie_api.config import get_settings;"
            "print(json.dumps(get_settings().effective_hook_claim_lease_s))"
        )
        worker = json.loads(
            self.kube(
                "exec",
                f"deployment/{FULLNAME}-worker",
                "--",
                "python3",
                "-c",
                "import json;from curie_worker.config import WorkerConfig;"
                "print(json.dumps(WorkerConfig().effective_hook_claim_lease_s))",
            )
        )
        assert api == worker == self.claim_lease_s
        self.worker(False)

    def wait_lease(self, run):
        return self.api_python(
            "import asyncio,json,os,select,time,uuid\n"
            "import asyncpg\nfrom curie_api.config import get_settings\n"
            "async def wait():\n"
            " c=await asyncpg.connect(get_settings().database_url.replace("
            "'postgresql+asyncpg://','postgresql://'))\n"
            " try:\n"
            "  while True:\n"
            "   remaining=await c.fetchval('SELECT extract(epoch FROM "
            "lease_expires_at-clock_timestamp()) FROM curie.hook_runs WHERE id=$1',"
            f"uuid.UUID('{run}'))\n"
            "   if remaining <= 0: break\n"
            "   fd=os.timerfd_create(time.CLOCK_MONOTONIC)\n"
            "   try:\n"
            "    os.timerfd_settime(fd,initial=float(remaining))\n"
            "    select.select([fd],[],[],float(remaining)+5)\n"
            "   finally: os.close(fd)\n"
            "  print(json.dumps({'expired_on_database_clock':True}))\n"
            " finally: await c.close()\n"
            "asyncio.run(wait())\n"
        )

    def fault_mode(self, mode):
        return json.loads(
            self.kube(
                "exec",
                "test-adr0274-wake-proxy",
                "--",
                "python3",
                "-c",
                "import json,urllib.request;"
                f"raw=json.dumps({{'mode':{mode!r}}}).encode();"
                "request=urllib.request.Request('http://127.0.0.1:8000/',data=raw);"
                "print(urllib.request.urlopen(request,timeout=10).read().decode())",
            )
        )

    @contextmanager
    def transport_fault(self):
        import webhook_wake_fault_proxy

        name = "test-adr0274-wake-proxy"
        host = self.api_python(
            "import json,urllib.parse;from curie_api.config import get_settings;"
            "print(json.dumps(urllib.parse.urlsplit(get_settings().valkey_dsn()).hostname))"
        ).split(".")[0]
        service = json.loads(self.kube("get", "service", host, "-o", "json"))
        endpoints = json.loads(self.kube("get", "endpoints", host, "-o", "json"))
        upstream = endpoints["subsets"][0]["addresses"][0]["ip"]
        resources = [
            {
                "apiVersion": "v1",
                "kind": "ConfigMap",
                "metadata": {"name": name},
                "data": {"proxy.py": Path(webhook_wake_fault_proxy.__file__).read_text()},
            },
            {
                "apiVersion": "v1",
                "kind": "Pod",
                "metadata": {
                    "name": name,
                    "labels": {
                        **service["spec"]["selector"],
                        "app.kubernetes.io/component": "wake-fault-proxy",
                        "app": name,
                    },
                },
                "spec": {
                    "containers": [
                        {
                            "name": "proxy",
                            "image": self.images["api"],
                            "imagePullPolicy": "Never",
                            "command": ["python3", "/fixture/proxy.py"],
                            "env": [{"name": "UPSTREAM_HOST", "value": upstream}],
                            "volumeMounts": [{"name": "fixture", "mountPath": "/fixture"}],
                            "readinessProbe": {"tcpSocket": {"port": 6379}, "periodSeconds": 1},
                        }
                    ],
                    "volumes": [{"name": "fixture", "configMap": {"name": name}}],
                },
            },
            {
                "apiVersion": "v1",
                "kind": "Service",
                "metadata": {"name": name},
                "spec": {"selector": {"app": name}, "ports": [{"port": 6379}]},
            },
        ]
        try:
            self.kube(
                "apply",
                "-f",
                "-",
                stdin=json.dumps({"apiVersion": "v1", "kind": "List", "items": resources}),
            )
            self.kube("wait", "--for=condition=Ready", f"pod/{name}", "--timeout=300s")
            self.kube("set", "env", f"deployment/{FULLNAME}-api", f"VALKEY_HOST={name}")
            self.kube("rollout", "status", f"deployment/{FULLNAME}-api", "--timeout=600s")
            yield
        finally:
            self.kube("set", "env", f"deployment/{FULLNAME}-api", f"VALKEY_HOST={host}")
            self.kube("rollout", "status", f"deployment/{FULLNAME}-api", "--timeout=600s")
            self.kube("delete", "pod,service,configmap", name, "--ignore-not-found", "--wait=true")

    def kube(self, *args, stdin=None):
        if stdin is not None:
            stdin = PrivateInput(stdin)
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

    @staticmethod
    def scheduler_code(offset_seconds):
        return (
            "import asyncio,os\n"
            "from datetime import UTC,datetime,timedelta\n"
            "from curie_worker.run import build\n"
            "from curie_worker.config import WorkerConfig\n"
            "from curie_worker.worker_lifecycle import WorkerResources\n"
            "async def tick():\n"
            " resources=WorkerResources()\n"
            " try:\n"
            "  runtime=build(WorkerConfig(),os.environ,resources=resources)\n"
            f"  now=datetime.now(UTC)+timedelta(seconds={offset_seconds})\n"
            "  await runtime.cron_loop.one_pass(now)\n"
            " finally: await resources.aclose()\n"
            "asyncio.run(tick())\n"
        )

    def scheduler_tick(self, offset_seconds=0):
        name = "test-adr0274-wake-scheduler"
        jobs = json.loads(self.kube("get", "jobs", "-o", "json"))["items"]
        if any(job["metadata"]["name"] == name for job in jobs):
            raise RuntimeError("scheduler probe already exists; preserve its owner")
        deployment = json.loads(self.kube("get", "deployment", f"{FULLNAME}-worker", "-o", "json"))
        template = deployment["spec"]["template"]
        template["spec"]["restartPolicy"] = "Never"
        containers = template["spec"]["containers"]
        assert len(containers) == 1 and containers[0]["name"] == "worker"
        worker = containers[0]
        worker["command"] = ["python3", "-c", self.scheduler_code(offset_seconds)]
        worker.pop("args", None)
        for probe in ("readinessProbe", "livenessProbe", "startupProbe"):
            worker.pop(probe, None)
        job = {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": name},
            "spec": {"backoffLimit": 0, "activeDeadlineSeconds": 90, "template": template},
        }
        created = False
        try:
            self.kube("create", "-f", "-", stdin=json.dumps(job))
            created = True
            self.kube("wait", "--for=condition=Complete", f"job/{name}", "--timeout=300s")
        finally:
            if created:
                (self.root / "scheduler-probe.log").write_text(self.kube("logs", f"job/{name}"))
                self.kube("delete", "job", name, "--wait=true")

    def capture_failure(self):
        command = self.root / "command-failure.log"
        if command.exists():
            (self.root / "failure-command.log").write_text(command.read_text())
        (self.root / "failure-model.json").write_text(
            json.dumps({"paths": self.model_paths, "bash_requests": self.bash_requests})
        )
        for role in ("api", "worker"):
            try:
                output = self.kube("logs", f"deployment/{FULLNAME}-{role}", "--tail=300")
            except RuntimeError:
                output = "Container logs were unavailable during failure capture."
            (self.root / f"failure-{role}.log").write_text(output)
        pods = json.loads(self.kube("get", "pods", "-o", "json"))["items"]
        (self.root / "failure-pods.json").write_text(json.dumps(pods))
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

    def reservation_key(self, agent, delivery):
        agent_id = self.agents[agent]
        generation = self.http(f"/agents/{agent_id}/hooks/{HOOK}/wake-secret", admin=True)[0][
            "body"
        ]["generation"]
        scope = self.api_python(
            "import hashlib,hmac,json;from curie_api.config import get_settings;"
            "print(json.dumps(hmac.new(get_settings().api_key.encode(),"
            "b'curie.hook.wake.scope.v1',hashlib.sha256).hexdigest()))"
        )
        return f"curie:hook-wake:v1:{scope}:{agent_id}:{HOOK}:{generation}:{delivery}"

    def wake_batch(self, agent="acme-wake-idle", count=8):
        key, agent_id = self.key(agent), self.agents[agent]
        headers = [
            signed_headers(key, agent_id, HOOK, uuid.uuid4().hex, int(time.time()))
            for _ in range(count)
        ]
        return self.api_python(
            "import concurrent.futures,json,urllib.request,urllib.error\n"
            f"headers={headers!r}\n"
            "def send(item):\n"
            f" request=urllib.request.Request('http://127.0.0.1:8000/hooks/{agent_id}/{HOOK}/wake',"
            "data=b'{}',headers=item)\n"
            " try: response=urllib.request.urlopen(request,timeout=30)\n"
            " except urllib.error.HTTPError as error: response=error\n"
            " with response: return {'status':response.status,'body':json.loads(response.read())}\n"
            f"with concurrent.futures.ThreadPoolExecutor(max_workers={count}) as pool:\n"
            " print(json.dumps(list(pool.map(send,headers))))\n"
        )

    def assert_refused(self, status, reason, **kwargs):
        before = self.snapshot()
        agent = self.agents[kwargs.get("agent", "acme-wake-idle")]
        pattern = f"curie:hook-wake:v1:*:{agent}:*"
        reservations = set(self.redis("KEYS", pattern))
        answers = self.wake(**kwargs)
        assert all(
            item["status"] == status and item["body"] == {"detail": reason} for item in answers
        )
        assert self.snapshot() == before
        assert not set(self.redis("KEYS", pattern)) - reservations

    def queued(self, agent="acme-wake-idle", **kwargs):
        answer = self.wake(agent, **kwargs)[0]
        assert answer["status"] == 202, answer
        assert set(answer["body"]) == {"status", "run_id"}
        assert answer["body"]["status"] == "queued"
        return answer["body"]["run_id"]

    def pause(self, agent, enabled):
        action = "pause" if enabled else "resume"
        answer = self.http(
            f"/schedules/{self.agents[agent]}/{HOOK}/{action}", method="POST", admin=True
        )[0]
        assert answer["status"] == 200
        assert answer["body"] == {"agent": agent, "name": HOOK, "paused": enabled}

    def run_record(self, agent, run):
        return json.loads(
            self.run(
                self.cli,
                "cluster",
                "hook",
                "--context",
                self.context,
                "--namespace",
                NAME,
                "--release",
                NAME,
                "record",
                agent,
                HOOK,
                run,
                "--json",
            )
        )

    def reset_hook_session(self, agent):
        pods = [pod for pod, claim in self.agent_callers() if claim["agent"] == agent]
        if not pods:
            return
        threads = self.sql(
            "SELECT COALESCE(json_agg(thread_key),'[]'::json) FROM curie.thread_transcripts "
            f"WHERE agent_id='{self.agents[agent]}'"
        )
        assert len(threads) == 1
        self.worker(True)
        reset = json.loads(
            self.run(
                self.cli,
                "cluster",
                "reset-thread",
                agent,
                "--thread-key",
                threads[0],
                "--context",
                self.context,
                "--namespace",
                NAME,
                "--release",
                NAME,
                "--yes",
                "--json",
            )
        )
        assert reset["released"] and reset["route_existed"]
        for pod in pods:
            self.kube("wait", "--for=delete", f"pod/{pod}", "--timeout=300s")

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

    def redeploy(self, agent, bundle, label):
        return json.loads(
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
                label,
                "--json",
            )
        )

    def sandbox_manifest(self, agent):
        pods = [pod for pod, claim in self.agent_callers() if claim["agent"] == agent]
        assert len(pods) == 1
        return json.loads(
            self.kube(
                "exec",
                pods[0],
                "-c",
                "runner",
                "--",
                "python3",
                "-c",
                "import json,os;from pathlib import Path;"
                "print((Path(os.environ['CURIE_PLUGIN_DIR'])/'.claude-plugin/plugin.json').read_text())",
            )
        )

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
        try:
            if request.session.testsfailed:
                runtime.capture_failure()
        finally:
            runtime.close()


def test_acceptance_1(qualification):
    """Receipt secrecy is one part of item 1; connector boot needs its own proof."""
    stack = qualification
    for agent, receipt in stack.receipts.items():
        assert receipt["wake_triggers"] == [
            {
                "name": HOOK,
                "path": f"/hooks/{stack.agents[agent]}/{HOOK}/wake",
                "mode": "targetless",
            }
        ]
        assert_no_key(receipt, stack.key(agent))
    assert stack.redis("XLEN", "curie:runs") == 0
    before = stack.snapshot()
    stack.scheduler_tick()
    assert stack.snapshot() == before == {"runs": [], "stream": []}


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
    stack.assert_refused(401, "invalid_wake_signature", key="incorrect-key")
    stack.assert_refused(409, "hook_paused")
    stack.pause("acme-wake-idle", False)
    agent = stack.agents["acme-wake-idle"]
    for method, suffix in (("GET", ""), ("POST", "/rotate")):
        route = f"/agents/{agent}/hooks/{HOOK}/wake-secret{suffix}"
        for path, body, status, detail in (
            (route, "{}", 422, "wake_body_invalid"),
            (route + "?override=", None, 400, "wake_query_forbidden"),
        ):
            answer = stack.http(path, method=method, admin=True, body=body)[0]
            assert answer["status"] == status
            assert answer["body"] == {"detail": detail}
            assert answer["cache"] == "no-store"
        answer = stack.http(route, method=method)[0]
        assert answer["status"] == 401
        assert answer["body"] == {"detail": "missing or invalid API key"}
        assert answer["cache"] == "no-store"
    bundle = stack.root / "acme-wake-idle"
    manifest_path = bundle / ".claude-plugin/plugin.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["triggers"].append(
        {
            "type": "webhook",
            "wake": True,
            "name": "queue-secondary",
            "path": "secondary",
            "prompt": "Read pending secondary items.",
        }
    )
    manifest_path.write_text(json.dumps(manifest))
    stack.redeploy("acme-wake-idle", bundle, "second-wake-hook")
    stack.assert_refused(401, "invalid_wake_signature", key=stack.key(hook="queue-secondary"))
    wake_path = f"/hooks/{agent}/{HOOK}/wake"
    saved = {
        "method": "POST",
        "body": BODY.decode(),
        "headers": signed_headers(key, agent, HOOK, uuid.uuid4().hex, int(time.time())),
    }
    admitted = stack.http(wake_path, **saved)[0]
    assert admitted["status"] == 202
    run = admitted["body"]["run_id"]
    rotated = stack.http(
        f"/agents/{agent}/hooks/{HOOK}/wake-secret/rotate", method="POST", admin=True
    )[0]
    assert rotated["status"] == 200 and rotated["cache"] == "no-store"
    assert set(rotated["body"]) == {"agent_id", "hook", "generation"}
    if stack.key() == key:
        raise AssertionError("rotation did not replace the current wake key")
    stack.assert_refused(401, "invalid_wake_signature", key=key)
    before_replay = stack.snapshot()
    replay = stack.http(wake_path, **saved)[0]
    assert replay["status"] == 401
    assert replay["body"] == {"detail": "invalid_wake_signature"}
    assert stack.snapshot() == before_replay
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
            else (
                "source_bindings",
                {
                    "workload_pointer": "/workload",
                    "map": {"acme": {"repository": "acme-corp/acme-bot", "revision": "main"}},
                },
            )
        )
        updated = stack.http(
            f"/agents/{agent_id}",
            method="PATCH",
            admin=True,
            body=json.dumps({column: {HOOK: value}}),
            headers={"Content-Type": "application/json"},
        )[0]
        assert updated["status"] == 200
    before = stack.snapshot()
    stack.assert_refused(401, "invalid_wake_signature", agent=agent, key="incorrect-key")
    stack.assert_refused(409, "wake_source_not_ordinary", agent=agent, key=key)
    route = f"/agents/{agent_id}/hooks/{HOOK}/wake-secret"
    for path, method in ((route, "GET"), (route + "/rotate", "POST")):
        response = stack.http(path, method=method, admin=True)[0]
        assert response["status"] == 409
        assert response["body"] == {"detail": "wake_source_not_ordinary"}
        assert response["cache"] == "no-store"
    assert stack.snapshot() == before
    assert (
        stack.sql(f"SELECT count(*) FROM curie.hook_wake_keys WHERE agent_id='{agent_id}'") == "0"
    )
    assert stack.http("/agents", admin=True)[0]["status"] == 200


def test_acceptance_3(qualification):
    stack = qualification
    stack.worker(False)
    delivery = uuid.uuid4().hex
    timestamp = int(time.time()) + 120
    answers = stack.wake(delivery=delivery, timestamp=timestamp, copies=8)
    assert sorted(item["status"] for item in answers) == [200] * 7 + [202]
    run = next(item["body"]["run_id"] for item in answers if item["status"] == 202)
    reservation = stack.reservation_key("acme-wake-idle", delivery)
    assert stack.redis("PTTL", reservation) >= 421_000
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


def test_signature_purposes_and_shared_key(qualification):
    stack = qualification
    stack.worker(False)
    agent = stack.agents["acme-wake-idle"]
    key = stack.key()
    shared = stack.http(f"/agents/{agent}/hook-secret", admin=True)[0]
    assert shared["status"] == 200
    ordinary_key = shared["body"]["secret"]
    stack.assert_refused(401, "invalid_wake_signature", key=ordinary_key)
    timestamp, delivery = int(time.time()), uuid.uuid4().hex
    headers = signed_headers(key, agent, HOOK, delivery, timestamp)
    before = stack.snapshot()
    headers["Content-Type"] = "application/json"
    crossed = stack.http(f"/hooks/{agent}/{HOOK}", method="POST", body="{}", headers=headers)[0]
    assert crossed["status"] == 401
    for signing_key in (ordinary_key, key):
        headers = ordinary_headers(signing_key, HOOK, delivery, timestamp)
        crossed = stack.http(
            f"/hooks/{agent}/{HOOK}/wake", method="POST", body="{}", headers=headers
        )[0]
        assert crossed["status"] == 401
        assert crossed["body"] == {"detail": "invalid_wake_signature"}
    assert stack.snapshot() == before
    run = stack.queued()
    stack.worker(True)
    stack.settled(run, "ran")


def test_fresh_overlap_and_dedupe_expiry(qualification):
    stack = qualification
    stack.worker(False)
    answers = stack.wake_batch()
    assert sorted(item["status"] for item in answers) == [200] * 7 + [202]
    run = next(item["body"]["run_id"] for item in answers if item["status"] == 202)
    before = stack.snapshot()
    assert stack.wake_batch() == [{"status": 200, "body": {"status": "coalesced"}}] * 8
    assert stack.snapshot() == before
    stack.worker(True)
    stack.settled(run, "ran")
    stack.worker(False)
    delivery, timestamp = uuid.uuid4().hex, int(time.time())
    run = stack.queued(delivery=delivery, timestamp=timestamp)
    reservation = stack.reservation_key("acme-wake-idle", delivery)
    assert 300_000 <= stack.redis("PTTL", reservation) <= 601_000
    stack.worker(True)
    stack.settled(run, "ran")
    stack.worker(False)
    before = stack.snapshot()
    assert stack.wake(delivery=delivery, timestamp=timestamp)[0]["body"] == {"status": "coalesced"}
    assert stack.snapshot() == before
    assert stack.redis("PEXPIRE", reservation, "1") == 1
    asyncio.run(
        wait_until(
            lambda: stack.redis("EXISTS", reservation) == 0,
            "reservation expiry",
            timeout=5,
            interval=0.1,
        )
    )
    fresh = stack.queued(delivery=delivery, timestamp=timestamp)
    assert fresh != run
    stack.worker(True)
    stack.settled(fresh, "ran")


def test_acceptance_4(qualification):
    """HTTP admission through real worker/sandbox; hosted MCP remains separate."""
    stack = qualification
    stack.worker(True)
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
    attempted = stack.bash_requests
    gated = stack.queued("acme-wake-gated")
    stack.settled(gated, "failed", "approval_gate_targetless")
    settled = stack.snapshot()
    stack.scheduler_tick()
    assert stack.snapshot() == settled
    assert stack.bash_requests > attempted
    assert stack.marker_value("acme-wake-gated") is None
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
    stack.worker(True)
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
    record = stack.run_record("acme-wake-idle", run)
    assert record["id"] == run
    assert record["source"] == record["trigger"] == "webhook"
    assert_no_key(record, stack.key())
    assert stack.wait_telemetry()["wakes"]


@pytest.mark.parametrize("action", ["rotate", "pause", "source"])
def test_admission_races(qualification, action):
    from pathlib import Path

    import webhook_wake_races

    stack = qualification
    agent = "acme-wake-idle"
    if action == "source":
        agent = "acme-wake-race-source"
        stack.add_agent(agent)
    delivery, timestamp = uuid.uuid4().hex, int(time.time())
    key = stack.key(agent)
    if action == "rotate":
        admitted = stack.queued(agent, delivery=delivery, timestamp=timestamp)
        stack.worker(True)
        stack.settled(admitted, "ran")
    stack.worker(False)
    before = stack.snapshot()
    spec = dict(
        agent=stack.agents[agent],
        hook=HOOK,
        action=action,
        headers=signed_headers(key, stack.agents[agent], HOOK, delivery, timestamp),
    )
    answers = stack.api_python(
        "SPEC="
        + repr(spec)
        + "\n"
        + Path(webhook_wake_races.__file__).read_text()
        + "\nasyncio.run(coordinate(SPEC))\n"
    )
    if action != "source":
        assert answers.pop(0)["status"] == 200
    expected = (
        (401, "invalid_wake_signature")
        if action == "rotate"
        else (409, "hook_paused" if action == "pause" else "wake_source_not_ordinary")
    )
    assert answers == [{"status": expected[0], "body": {"detail": expected[1]}}] * 4
    assert stack.snapshot() == before
    if action == "pause":
        stack.pause(agent, False)
    if action != "source":
        later = stack.queued(agent)
        stack.worker(True)
        stack.settled(later, "ran")


def test_installed_migration(qualification):
    import webhook_wake_migration

    observed = qualification.api_python(
        "import asyncio\n"
        + Path(webhook_wake_migration.__file__).read_text()
        + "\nasyncio.run(qualify())\n"
    )
    assert observed == {
        "history_preserved": True,
        "webhook_accepted": True,
        "lossy_downgrades_refused": 2,
        "clean_downgrade": "0148",
    }


def test_stale_delivery_and_operator_controls(qualification):
    from aci_protocol import QueuedTurn

    stack = qualification
    agent = stack.agents["acme-wake-idle"]
    run = stack.queued()
    stack.worker(True)
    stack.settled(run, "ran")
    before = stack.snapshot()
    event = QueuedTurn.model_validate_json(before["stream"][-1][1]["payload"])
    stale = event.model_copy(update={"event_id": event.event_id + ":stale"})
    stack.enqueue(stale)
    stack.wait_done(stale)
    assert stack.snapshot()["runs"] == before["runs"]
    assert stack.redis("XLEN", "curie:runs") == len(before["stream"]) + 1
    stack.worker(False)
    queued = stack.queued()
    key = stack.key()
    route = f"/agents/{agent}/hooks/{HOOK}/runs/{queued}"
    assert stack.http(route)[0]["status"] in (401, 403)
    assert stack.http(route, headers={"X-Api-Key": key})[0]["status"] in (401, 403)
    assert (
        stack.http(f"/agents/{agent}/hooks/{HOOK}/wake-secret/rotate", method="POST", admin=True)[
            0
        ]["status"]
        == 200
    )
    assert stack.http(f"/agents/{agent}/kill", method="POST", admin=True)[0]["body"] == {
        "killed": True
    }
    stack.assert_refused(401, "invalid_wake_signature", key="incorrect-key")
    stack.assert_refused(409, "agent_killed")
    stack.worker(True)
    stack.settled(queued, "blocked", "agent_killed")
    queried = stack.http(route, admin=True)[0]
    assert queried["status"] == 200 and queried["body"]["trigger"] == "webhook"
    assert stack.http(f"/agents/{agent}/resume", method="POST", admin=True)[0]["body"] == {
        "killed": False
    }
    fresh = stack.queued()
    stack.settled(fresh, "ran")
    assert stack.slack_calls == []


def test_deployment_replacement_and_cron_overlap(qualification):
    stack = qualification
    agent = "acme-wake-replacement"
    stack.add_agent(agent)
    stack.worker(False)
    run = stack.queued(agent)
    accepted_version = stack.sql(f"SELECT version_id::text FROM curie.hook_runs WHERE id='{run}'")
    bundle = stack.root / agent
    path = bundle / ".claude-plugin/plugin.json"
    original = json.loads(path.read_text())
    replacement = {**original, "version": "1.0.1"}
    path.write_text(json.dumps(replacement))
    stack.redeploy(agent, bundle, "replacement")
    stack.worker(True)
    stack.settled(run, "ran")
    recorded = stack.sql(f"SELECT version_id::text FROM curie.hook_runs WHERE id='{run}'")
    assert recorded == accepted_version
    assert stack.sandbox_manifest(agent)["version"] == "1.0.1"
    stack.worker(False)
    overlap = stack.queued(agent)
    replacement["version"] = "1.0.2"
    replacement["triggers"] = [
        {
            "type": "cron",
            "name": HOOK,
            "schedule": "* * * * *",
            "prompt": original["triggers"][0]["prompt"],
        }
    ]
    path.write_text(json.dumps(replacement))
    try:
        stack.redeploy(agent, bundle, "cron-overlap")
        stack.worker(False)
        entries = stack.redis("XLEN", "curie:runs")
        stack.scheduler_tick(offset_seconds=65)
        rows = stack.sql(
            "SELECT json_agg(json_build_object('source',source,'outcome',outcome,'reason',reason)) "
            f"FROM curie.hook_runs WHERE agent_id='{stack.agents[agent]}' AND source='schedule'"
        )
        assert {"source": "schedule", "outcome": "skipped", "reason": "run_in_flight"} in rows
        assert stack.redis("XLEN", "curie:runs") == entries
        stack.worker(True)
        stack.settled(overlap, "ran")
    finally:
        original["version"] = "1.0.3"
        path.write_text(json.dumps(original))
        stack.redeploy(agent, bundle, "restore-wake")
    assert stack.slack_calls == []


@pytest.mark.parametrize("budget,lease", [(60, 75), (65, None)])
def test_transport_failure_and_timed_recovery(qualification, budget, lease):
    from curie_worker.sweep import HOOK_LEASE_START_MARGIN_S

    stack = qualification
    stack.configure_lease(budget, lease)
    try:
        with stack.transport_fault():
            before = stack.snapshot()
            key = stack.key()
            assert stack.fault_mode("forward")["forwarded"] > 0
            stack.fault_mode("down")
            try:
                answer = stack.wake(key=key)[0]
            finally:
                observed = stack.fault_mode("forward")
            assert observed["refused"] > 0, observed
            assert answer["status"] == 503, answer
            assert answer["body"] == {"detail": "authority_unavailable"}
            assert stack.snapshot() == before
            stack.fault_mode("refuse_set_nx")
            try:
                answer = stack.wake(key=key)[0]
            finally:
                reservation_fault = stack.fault_mode("forward")
            assert reservation_fault["refused"] > observed["refused"], reservation_fault
            assert answer["status"] == 503, answer
            assert answer["body"] == {"detail": "wake_dedupe_unavailable"}
            assert stack.snapshot() == before
            stack.fault_mode("drop_xadd")
            try:
                answer = stack.wake()[0]
            finally:
                observed = stack.fault_mode("forward")
            assert observed["dropped"] > 0, observed
            assert answer["status"] == 503, answer
            assert answer["body"] == {"detail": "wake_enqueue_unavailable"}
            rows = stack.sql(
                "SELECT json_agg(json_build_object('id',id,'outcome',outcome,'lease',"
                "extract(epoch FROM lease_expires_at-started_at))) FROM curie.hook_runs "
                "WHERE outcome IS NULL"
            )
            assert len(rows) == 1 and rows[0]["outcome"] is None
            assert abs(rows[0]["lease"] - stack.claim_lease_s) < 1
            assert stack.snapshot()["stream"] == before["stream"]
            lost = rows[0]["id"]
            stack.restart_api()
            before_expiry = stack.snapshot()
            assert stack.wake()[0]["body"] == {"status": "coalesced"}
            assert stack.snapshot() == before_expiry
            assert stack.wait_lease(lost) == {"expired_on_database_clock": True}
            fresh = stack.queued()
            stack.settled(lost, "reclaimed", "claim_expired")
            stack.worker(True)
            assert stack.settled(fresh, "ran")["renewed"]
            remaining = stack.sql(
                "SELECT extract(epoch FROM lease_expires_at-clock_timestamp()) "
                f"FROM curie.hook_runs WHERE id='{fresh}'"
            )
            assert 0 < float(remaining) <= stack.claim_lease_s + HOOK_LEASE_START_MARGIN_S + 1
    finally:
        stack.configure_lease(600, None)
        stack.worker(True)


@pytest.mark.parametrize("qualification", ["cluster"], indirect=True)
@pytest.mark.parametrize("agent", ["acme-wake-idle", "acme-wake-http"])
@pytest.mark.parametrize(
    "provider_tier",
    [
        "scripted",
        pytest.param(
            "live",
            marks=pytest.mark.skipif(
                not os.environ.get("WAKE_TEST_MODEL_CREDENTIAL"),
                reason=LIVE_PROVIDER_GAP,
            ),
        ),
    ],
)
def test_hosted_connector_session(qualification, agent, provider_tier):
    """Read and write through the real MCP path, with independent model tiers."""
    stack = qualification
    live = provider_tier == "live"
    credential = os.environ.get("WAKE_TEST_MODEL_CREDENTIAL", "") if live else "not-needed"
    agent_id = stack.agents[agent]
    stack.worker(True)
    warm_run = stack.queued(agent)
    stack.settled(warm_run, "ran")
    assert any(claim["agent"] == agent for _, claim in stack.agent_callers())
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
    stack.run("kind", "load", "docker-image", "--name", stack.cluster, image, timeout=600)
    wake_secret = f"{agent}-wake-credential"
    for name, value in ((wake_secret, key), ("acme-model-credential", credential)):
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
    base_url = os.environ.get("WAKE_TEST_MODEL_BASE_URL", "") if live else stack.model_url
    allowed_egress = None
    if live:
        installed = json.loads(
            stack.run(
                "helm",
                "get",
                "values",
                NAME,
                "--kube-context",
                stack.context,
                "--namespace",
                NAME,
                "-o",
                "json",
            )
        )
        # The second live case reuses the first one's values; skip entries already present.
        allowed_egress = installed["security"]["networkPolicy"]["allowedEgress"]
        for entry in live_model_egress(
            base_url, os.environ.get("WAKE_TEST_MODEL_EGRESS_CIDRS", "")
        ):
            if entry not in allowed_egress:
                allowed_egress.append(entry)
    values_path = stack.root / "live.yaml"
    values_path.write_text(
        yaml.safe_dump(
            provider_tier_values(
                base_url=base_url,
                model=os.environ.get("WAKE_TEST_MODEL", "") if live else "",
                credential_secret="acme-model-credential",
                allowed_egress=allowed_egress,
            )
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
        str(values_path),
        "--wait",
        "--timeout",
        "900s",
        timeout=960,
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
                            "WAKE_URL": f"http://{FULLNAME}-api:8000/hooks/{agent_id}/{HOOK}/wake",
                            "WAKE_QUEUE_DB": f"/tmp/wake-queue-{provider_tier}.sqlite3",
                        },
                        "secrets": [
                            {
                                "name": "CURIE_HOOK_WAKE_KEY",
                                "from_secret": wake_secret,
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
            and any(agent_id in env.get("value", "") for env in container.get("env", []))
            for container in item["spec"]["template"]["spec"]["containers"]
        )
    ]
    assert len(connectors) == 1
    connector = connectors[0]
    stack.kube("rollout", "status", f"deployment/{connector}", "--timeout=600s")
    deployment = json.loads(stack.kube("get", "deployment", connector, "-o", "json"))
    assert_no_key(deployment, key)
    pod_spec = deployment["spec"]["template"]["spec"]
    references = [
        container["name"]
        for container in pod_spec["containers"] + pod_spec.get("initContainers", [])
        if wake_secret in json.dumps(container)
    ]
    assert references == ["server"]
    stack.reset_hook_session(agent)

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

    def sandbox_ids():
        return {
            item["metadata"]["uid"]
            for item in json.loads(stack.kube("get", "sandboxes", "-o", "json"))["items"]
        }

    sandboxes = sandbox_ids()
    assert control("/poll", {}) == {"status": "empty"}
    assert stack.snapshot() == before
    assert sandbox_ids() == sandboxes
    if not live:
        stack.worker(False)
        control("/barriers", {"read": True, "complete": True})
    first_item, late_item = f"{provider_tier}-item-1", f"{provider_tier}-item-2"
    control("/items", {"id": first_item, "text": "Record the result receipt-ok."})
    polled = control("/poll", {})
    assert polled["http_status"] == 202
    run = polled["receipt"]["run_id"]
    if live:
        stack.worker(True)
        stack.settled(run, "ran")
    else:
        try:
            admitted = stack.snapshot()
            assert control("/poll", {})["receipt"] == {"status": "coalesced"}
            assert stack.snapshot() == admitted
            stack.worker(True)
            asyncio.run(
                wait_until(
                    lambda: control("/observation")["read_waiting"],
                    "connector's last read",
                    timeout=90,
                    interval=0.25,
                )
            )
            control("/items", {"id": late_item, "text": "Arrived after the last queue read."})
            assert control("/poll", {})["receipt"] == {"status": "coalesced"}
            control("/barriers", {"read": False, "complete": True})
            asyncio.run(
                wait_until(
                    lambda: control("/observation")["complete_waiting"],
                    "connector's last tool call before settlement",
                    timeout=30,
                    interval=0.25,
                )
            )
            assert control("/poll", {})["receipt"] == {"status": "coalesced"}
        finally:
            control("/barriers", {"read": False, "complete": False})
        stack.settled(run, "ran")
        after = stack.snapshot()
        assert {row["id"] for row in after["runs"]} == {row["id"] for row in admitted["runs"]}
        assert after["stream"] == admitted["stream"]
        assert control("/observation")["items"] == [
            {"id": first_item, "result": "receipt-ok"},
            {"id": late_item, "result": None},
        ]
        fresh = control("/poll", {})
        assert fresh["http_status"] == 202
        stack.settled(fresh["receipt"]["run_id"], "ran")
    observation = control("/observation")
    expected = [{"id": first_item, "result": "receipt-ok"}]
    if not live:
        expected.append({"id": late_item, "result": "receipt-ok"})
    assert observation["items"] == expected
    assert {call["tool"] for call in observation["calls"]} == {"read", "complete"}
    assert all(call["agent"] == agent for call in observation["calls"])
    assert all(not call["human_header"] and not call["key_header"] for call in observation["calls"])
    assert_no_key(stack.transcripts(agent), key)
    assert_no_key(observation, key)
    assert_no_key(polled, key)
    assert_no_key(json.loads(stack.kube("get", "sandboxtemplates", "-o", "json")), key)
    for pod, claims in stack.agent_callers():
        if claims["agent"] != agent:
            continue
        inspection = stack.kube(
            "exec",
            pod,
            "-c",
            "runner",
            "--",
            "python3",
            "-c",
            "import json,os;from pathlib import Path;"
            "print(json.dumps({'env':dict(os.environ),'configs':{str(p):p.read_text() "
            "for p in Path(os.environ['CURIE_PLUGIN_DIR']).rglob('*.json')}}))",
        )
        assert_no_key(json.loads(inspection), key)
        wake_key_present = "CURIE_HOOK_WAKE_KEY" in json.loads(inspection)["env"]
        assert not wake_key_present
        spec = json.loads(stack.kube("get", "pod", pod, "-o", "json"))["spec"]
        for container in spec["containers"] + spec.get("initContainers", []):
            assert_no_key(stack.kube("logs", pod, "-c", container["name"]), key)
    for role in ("api", "worker"):
        assert_no_key(stack.kube("logs", f"deployment/{FULLNAME}-{role}"), key)
    connector_pods = [
        pod
        for pod in json.loads(stack.kube("get", "pods", "-o", "json"))["items"]
        if pod["metadata"]["name"].startswith(connector + "-")
    ]
    assert connector_pods
    for pod in connector_pods:
        for container in pod["spec"]["containers"] + pod["spec"].get("initContainers", []):
            assert_no_key(stack.kube("logs", pod["metadata"]["name"], "-c", container["name"]), key)
    snapshot = stack.wait_telemetry()
    assert snapshot["wakes"]
    assert any(
        agent.encode() in raw or agent_id.encode() in raw
        for path, raw in stack.exports
        if path.endswith("/traces")
    )
    for _, exported in stack.exports:
        if key.encode() in exported:
            raise AssertionError("wake credential escaped into telemetry")
    assert stack.slack_calls == []
