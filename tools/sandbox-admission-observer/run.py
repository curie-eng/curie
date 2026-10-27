"""Observe shared admission while the unchanged quota runtime assertion runs."""

from __future__ import annotations

import json
import os
import queue
import re
import signal
import subprocess
import sys
import threading
import time
from typing import Any
from urllib.parse import urlencode


class ObservationError(RuntimeError):
    """A fixed, safe failure that never includes observed object contents."""


class AdmissionObserver:
    def __init__(self) -> None:
        self.max_claims = 0
        self.max_sandboxes = 0
        self.max_reservations = 0
        self._saw_claim = False
        self._saw_sandbox = False
        self._saw_ledger = False
        self._saw_refusal = False
        self._violation: str | None = None

    def _fail(self, message: str) -> None:
        self._violation = message
        raise ObservationError(message)

    def observe(
        self,
        *,
        claims: list[dict[str, Any]],
        sandboxes: list[dict[str, Any]],
        ledger: dict[str, Any] | None,
        worker_logs: list[dict[str, Any]],
    ) -> None:
        for objects in (claims, sandboxes):
            if not isinstance(objects, list) or any(not isinstance(row, dict) for row in objects):
                self._fail("invalid admission object observation")
            if len(objects) > 2:
                self._fail("observed more than two claims or Sandboxes")
            for obj in objects:
                status = obj.get("status", {})
                if not isinstance(status, dict):
                    self._fail("invalid admission object status")
                conditions = status.get("conditions", [])
                if not isinstance(conditions, list):
                    self._fail("invalid admission object conditions")
                for condition in conditions:
                    if not isinstance(condition, dict):
                        self._fail("invalid admission object condition")
                    if (
                        condition.get("type") == "Ready"
                        and condition.get("status") == "False"
                        and "exceeded quota" in str(condition.get("message", "")).lower()
                    ):
                        self._fail("observed a Ready condition reporting quota rejection")
        self.max_claims = max(self.max_claims, len(claims))
        self.max_sandboxes = max(self.max_sandboxes, len(sandboxes))
        self._saw_claim |= bool(claims)
        self._saw_sandbox |= bool(sandboxes)
        if ledger is not None:
            if not isinstance(ledger, dict):
                self._fail("invalid admission ledger observation")
            if len(ledger) > 2:
                self._fail("observed more than two admission reservations")
            for row in ledger.values():
                if (
                    not isinstance(row, dict)
                    or row.get("state") not in ("preparing", "submitted", "bound", "retiring")
                    or not isinstance(row.get("owner_token"), str)
                    or not row["owner_token"]
                    or not isinstance(row.get("resources"), dict)
                    or not {
                        "pods",
                        "requests.cpu",
                        "requests.memory",
                        "limits.cpu",
                        "limits.memory",
                    }.issubset(row["resources"])
                ):
                    self._fail("invalid admission reservation observation")
            self.max_reservations = max(self.max_reservations, len(ledger))
            self._saw_ledger = True
        if not isinstance(worker_logs, list):
            self._fail("invalid worker log observation")
        for record in worker_logs:
            if not isinstance(record, dict):
                self._fail("invalid worker log record")
            message = record.get("message")
            if (
                message == "sandbox admission refused"
                and record.get("evidence_type") == "admission"
            ):
                self._saw_refusal = True
            typed_message = "sandbox admission refused evidence_type=admission"
            if isinstance(message, str) and (
                message == typed_message or message.startswith(typed_message + " ")
            ):
                self._saw_refusal = True

    def verify(self) -> None:
        if self._violation:
            raise ObservationError(self._violation)
        if not self._saw_claim or not self._saw_sandbox:
            raise ObservationError("missing observed claim or Sandbox")
        if not self._saw_ledger:
            raise ObservationError("missing namespace admission ledger")
        if not self._saw_refusal:
            raise ObservationError("missing typed admission refusal")


_LEDGER_PROBE = r"""
import json, sys, time
import redis
from curie_internal.keyspace import SANDBOX_KEY_PREFIX_DEFAULT
from curie_worker.config import WorkerConfig
try:
    config = WorkerConfig()
    client = redis.Redis(**config.valkey_client_kwargs(),
                         socket_timeout=5, socket_connect_timeout=5)
    key = f"{SANDBOX_KEY_PREFIX_DEFAULT}:admission:{sys.argv[1]}:{sys.argv[2]}"
    while True:
        with client.pipeline() as pipe:
            exists, rows = pipe.exists(key).hgetall(key).execute()
        ledger = {name: json.loads(raw) for name, raw in rows.items()} if exists else None
        print(json.dumps({"ledger": ledger}), flush=True)
        time.sleep(0.2)
except Exception:
    print(json.dumps({"error": "ledger observation failed"}), flush=True)
    sys.exit(1)
"""

_KINDS = {
    "claims": "sandboxclaims.extensions.agents.x-k8s.io",
    "sandboxes": "sandboxes.agents.x-k8s.io",
}


class _Collector:
    def __init__(self, context: str, run_id: str, command: list[str]) -> None:
        self.namespace = f"test-4368-idle-capacity-{run_id}"
        self.fullname = f"idlecap-{run_id}-curie"
        self.kube = ["kubectl", "--context", context, "--namespace", self.namespace]
        self.command = command
        self.observer = AdmissionObserver()
        self.child: subprocess.Popen[Any] | None = None
        self.interrupted_child = False
        self._streams: list[subprocess.Popen[str]] = []
        self._readers: list[threading.Thread] = []
        self._events: queue.Queue[tuple[str, str, Any]] = queue.Queue()
        self._objects: dict[str, dict[str, dict[str, Any]]] = {kind: {} for kind in _KINDS}
        self._versions: dict[str, str] = {}
        self._ledger: dict[str, Any] | None = None
        self._namespace_present = False
        self._namespace_ended = False
        self._worker_started = False
        self._verified_live = False
        self._deleting: dict[tuple[str, str], float] = {}
        self.deletion_count = 0
        self.max_deletion_seconds = 0.0

    def _request_json(self, *args: str) -> dict[str, Any]:
        result = subprocess.run(
            [*self.kube, *args, "--request-timeout=5s"],
            capture_output=True,
            text=True,
            check=False,
            timeout=10,
        )
        if result.returncode:
            raise ObservationError("admission control plane observation failed")
        document = json.loads(result.stdout) if result.stdout.strip() else {}
        if not isinstance(document, dict):
            raise ObservationError("invalid admission control plane response")
        return document

    def _get(self, *args: str) -> dict[str, Any]:
        return self._request_json("get", *args, "--ignore-not-found", "-o", "json")

    def _stream(self, source: str, arguments: list[str]) -> None:
        process = subprocess.Popen(
            [*self.kube, *arguments],
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            start_new_session=True,
        )
        self._streams.append(process)

        def read() -> None:
            assert process.stdout is not None
            buffer = ""
            decoder = json.JSONDecoder()
            try:
                for line in process.stdout:
                    if source == "logs":
                        try:
                            document = json.loads(line)
                        except ValueError:
                            continue
                        if isinstance(document, dict):
                            self._events.put(("data", source, document))
                        continue
                    buffer += line
                    if len(buffer) > 8 * 1024 * 1024:
                        raise ValueError
                    while buffer.strip():
                        buffer = buffer.lstrip()
                        try:
                            document, end = decoder.raw_decode(buffer)
                        except ValueError:
                            break
                        self._events.put(("data", source, document))
                        buffer = buffer[end:]
                if buffer.strip():
                    raise ValueError
            except (OSError, UnicodeError, ValueError):
                self._events.put(("error", source, None))
            finally:
                self._events.put(("end", source, process.wait()))

        reader = threading.Thread(target=read, daemon=True)
        self._readers.append(reader)
        reader.start()

    def _resource_path(self, source: str) -> str:
        resource, group = _KINDS[source].split(".", 1)
        return f"/apis/{group}/v1beta1/namespaces/{self.namespace}/{resource}"

    def _watch(self, source: str) -> None:
        query = urlencode({"watch": "true", "resourceVersion": self._versions[source]})
        self._stream(source, ["get", "--raw", f"{self._resource_path(source)}?{query}"])

    def _start_objects(self) -> None:
        for source in _KINDS:
            initial = self._request_json("get", "--raw", self._resource_path(source))
            items = initial.get("items")
            metadata = initial.get("metadata")
            version = metadata.get("resourceVersion") if isinstance(metadata, dict) else None
            if not isinstance(items, list) or not isinstance(version, str) or not version:
                raise ObservationError("invalid initial admission inventory")
            for obj in items:
                self._object(source, "ADDED", obj)
            self._versions[source] = version
            self._watch(source)
        self._observe([])

    def _object(self, source: str, event: str, obj: Any) -> None:
        if not isinstance(obj, dict) or event not in {"ADDED", "MODIFIED", "DELETED", "BOOKMARK"}:
            raise ObservationError("invalid admission inventory watch event")
        metadata = obj.get("metadata")
        if not isinstance(metadata, dict):
            raise ObservationError("invalid admission inventory metadata")
        version = metadata.get("resourceVersion")
        if isinstance(version, str) and version:
            self._versions[source] = version
        if event == "BOOKMARK":
            return
        identity = metadata.get("uid") or metadata.get("name")
        if not isinstance(identity, str) or not identity:
            raise ObservationError("missing admission inventory identity")
        key = (source, identity)
        if metadata.get("deletionTimestamp"):
            self._deleting.setdefault(key, time.monotonic())
        if event == "DELETED":
            # Check the final condition even when creation and deletion share a queue drain.
            self._objects[source][identity] = obj
            self._observe([])
            self._objects[source].pop(identity)
            started = self._deleting.pop(key, None)
            if started is not None:
                self.deletion_count += 1
                self.max_deletion_seconds = max(
                    self.max_deletion_seconds, time.monotonic() - started
                )
        else:
            self._objects[source][identity] = obj

    def _observe(self, logs: list[dict[str, Any]]) -> None:
        self.observer.observe(
            claims=list(self._objects["claims"].values()),
            sandboxes=list(self._objects["sandboxes"].values()),
            ledger=self._ledger,
            worker_logs=logs,
        )
        if self._namespace_present and not self._namespace_ended:
            try:
                self.observer.verify()
            except ObservationError:
                pass
            else:
                self._verified_live = True

    def _poll_startup(self) -> None:
        namespace = self._get("namespace", self.namespace)
        if not namespace:
            if self._namespace_present:
                self._namespace_ended = True
            return
        self._namespace_present = True
        if namespace.get("metadata", {}).get("deletionTimestamp"):
            self._namespace_ended = True
            self.observer.verify()
            return
        if not self._versions:
            crds = self._get("crd", *_KINDS.values())
            established = [
                any(
                    row.get("type") == "Established" and row.get("status") == "True"
                    for row in obj.get("status", {}).get("conditions", [])
                )
                for obj in crds.get("items", [])
            ]
            if len(established) == 2 and all(established):
                preflight = self._get("job", f"{self.fullname}-preflight-controller")
                if any(
                    row.get("type") == "Complete" and row.get("status") == "True"
                    for row in preflight.get("status", {}).get("conditions", [])
                ):
                    self._start_objects()
        if self._versions and not self._worker_started:
            worker = self._get("deployment", f"{self.fullname}-worker")
            if worker.get("status", {}).get("availableReplicas", 0) > 0:
                self._stream(
                    "logs",
                    ["logs", f"deployment/{self.fullname}-worker",
                     "--container", "worker", "--follow"],
                )
                self._stream(
                    "ledger",
                    [
                        "exec", f"deployment/{self.fullname}-worker", "--container", "worker",
                        "--", "python3", "-u", "-c", _LEDGER_PROBE, self.namespace,
                        f"{self.fullname}-sandbox-quota",
                    ],
                )
                self._worker_started = True

    def _ledger_teardown(self) -> bool:
        if not self._verified_live:
            return False
        self.observer.verify()
        for kind, name in (
            ("namespace", self.namespace),
            ("deployment", f"{self.fullname}-worker"),
            ("statefulset", f"{self.fullname}-valkey"),
        ):
            obj = self._get(kind, name)
            if not obj or obj.get("metadata", {}).get("deletionTimestamp"):
                return True
        return False

    def _event(self, kind: str, source: str, payload: Any, *, finished: bool = False) -> None:
        if kind == "error":
            raise ObservationError("invalid admission observation stream")
        if kind == "end":
            if finished or self._namespace_ended:
                return
            namespace = self._get("namespace", self.namespace)
            if not namespace or namespace.get("metadata", {}).get("deletionTimestamp"):
                self.observer.verify()
                self._namespace_ended = True
                return
            if source in _KINDS and payload == 0:
                self._watch(source)
                return
            if source == "ledger" and self._ledger_teardown():
                return
            if source in {"logs", "ledger"}:
                worker = self._get("deployment", f"{self.fullname}-worker")
                if not worker or worker.get("metadata", {}).get("deletionTimestamp"):
                    self.observer.verify()
                    return
            raise ObservationError("admission observation stream ended before cleanup")
        if not isinstance(payload, dict):
            raise ObservationError("invalid admission observation document")
        logs = []
        if source in _KINDS:
            self._object(source, payload.get("type", ""), payload.get("object"))
        elif source == "ledger":
            if payload == {"error": "ledger observation failed"} and self._ledger_teardown():
                return
            if "error" in payload or "ledger" not in payload:
                raise ObservationError("admission ledger observation failed")
            self._ledger = payload["ledger"]
        elif source == "logs":
            logs = [payload]
        self._observe(logs)

    @staticmethod
    def _terminate(process: subprocess.Popen[Any]) -> None:
        if process.poll() is None:
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass

    def _close_streams(self) -> None:
        for process in self._streams:
            self._terminate(process)
        for process in self._streams:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
        for reader in self._readers:
            reader.join(timeout=5)
            if reader.is_alive():
                raise ObservationError("admission observation reader did not stop")

    def close(self) -> None:
        if self.child is not None and self.child.poll() is None:
            self.interrupted_child = True
            self._terminate(self.child)
            self.child.wait()
        self._close_streams()

    def run(self) -> int:
        self.child = subprocess.Popen(self.command, start_new_session=True)
        next_poll = 0.0
        while self.child.poll() is None:
            if time.monotonic() >= next_poll:
                self._poll_startup()
                next_poll = time.monotonic() + 0.5
            try:
                event = self._events.get(timeout=0.1)
            except queue.Empty:
                continue
            self._event(*event)
        self._close_streams()
        while not self._events.empty():
            self._event(*self._events.get_nowait(), finished=True)
        if self.child.returncode:
            return self.child.returncode
        self.observer.verify()
        if not self._verified_live:
            raise ObservationError("admission proof was not verified before namespace cleanup")
        print("PASS: observed shared admission before namespace cleanup")
        print(
            f"maximum claims={self.observer.max_claims} Sandboxes={self.observer.max_sandboxes} "
            f"reservations={self.observer.max_reservations}"
        )
        print(
            f"observed deletions={self.deletion_count} "
            f"maximum observed deletion-to-absence seconds={self.max_deletion_seconds:.3f}"
        )
        return 0


def main(arguments: list[str] | None = None) -> int:
    arguments = sys.argv[1:] if arguments is None else arguments
    if len(arguments) < 2 or arguments[0] != "--":
        print("usage: sandbox-admission-observer -- <runtime gate command>", file=sys.stderr)
        return 2
    context = os.environ.get("CURIE_SCENARIO_CONTEXT", "")
    run_id = os.environ.get("CURIE_SCENARIO_RUN_ID", "")
    if not context or context.startswith("-") or re.search(r"[\s\x00-\x1f\x7f]", context):
        print("FAIL: a valid scenario context is required", file=sys.stderr)
        return 2
    if not re.fullmatch(r"[a-z0-9][a-z0-9-]{0,24}", run_id):
        print("FAIL: a valid scenario run ID is required", file=sys.stderr)
        return 2
    collector = _Collector(context, run_id, arguments[1:])

    def interrupted(_number: int, _frame: Any) -> None:
        raise ObservationError("admission observation interrupted")

    previous = signal.signal(signal.SIGTERM, interrupted)
    result = 1
    try:
        result = collector.run()
    except ObservationError as error:
        print(f"FAIL: {error}", file=sys.stderr)
    except KeyboardInterrupt:
        print("FAIL: admission observation interrupted", file=sys.stderr)
        result = 130
    except Exception:  # noqa: BLE001 - control plane exceptions may contain credentials
        print("FAIL: admission observer could not complete", file=sys.stderr)
    finally:
        signal.signal(signal.SIGTERM, previous)
        try:
            collector.close()
        except Exception:  # noqa: BLE001 - cleanup errors must not publish captured data
            print("FAIL: admission observer cleanup failed", file=sys.stderr)
            result = 1
    if (
        collector.child is not None
        and collector.child.returncode
        and not collector.interrupted_child
    ):
        code = collector.child.returncode
        return code if code > 0 else 128 - code
    return result


if __name__ == "__main__":
    raise SystemExit(main())
