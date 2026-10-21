"""Probe programs, pod templates and log parsers of the managed CNI probe kit, ADR 0238.

@spec MANAGED-CNI-2. Two programs run in probe pods from the pinned probe
image: an owned listener and a one-shot sender. Each prints JSON lines to its
own log, which the measuring identity reads through ``pods/log``; it holds no
exec, attach or port-forward right. The programs are fixed text, so the probe
kit digest covers them.

Every template has no Secret volume, no service account token, no host
namespace, no host path, no privileged container and a read-only root
filesystem, and it drops every capability. No template here adds ``NET_RAW``:
the spoofing template belongs to a later slice. The measuring identity chooses
only the node, the labels the template allows and the probe arguments.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from collections.abc import Mapping, Sequence, Set
from dataclasses import dataclass
from typing import Any

from .managed_cni_probe_kit import canonical_json, refuse, require

# The repository's pinned Python base image, by digest.
PROBE_IMAGE = (
    "python:3.13.15-slim@sha256:9d2e5553305c7c7b0097999bb17187c69b921ccd6bc9d40e4bb5ebe652c00285"
)
TEMPLATE_LABEL = "curietech.ai/managed-cni-probe-template"
PROBE_LABEL = "curietech.ai/managed-cni-probe"
CAMPAIGN_LABEL = "curietech.ai/managed-cni-campaign"
LOG_CAP = 8 * 1024 * 1024

LISTENER_PROGRAM = r"""
import json, socket, sys, threading, time
cfg = json.loads(sys.argv[1])
TCP, UDP = cfg["tcp"], cfg["udp"]
lock = threading.Lock()
def emit(doc):
    doc["t"] = int(time.time() * 1000)
    with lock:
        sys.stdout.write(json.dumps(doc, sort_keys=True) + "\n")
        sys.stdout.flush()
def norm(a):
    return a[7:] if a.startswith("::ffff:") else a
def bind(kind, port):
    try:
        s = socket.socket(socket.AF_INET6, kind)
        s.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("::", port))
    except OSError:
        s = socket.socket(socket.AF_INET, kind)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(("0.0.0.0", port))
    return s
def nonce(data):
    return data[:64].decode("ascii", "replace").strip()
def tcp(port):
    s = bind(socket.SOCK_STREAM, port)
    s.listen(64)
    while True:
        c, a = s.accept()
        c.settimeout(1.0)
        data = b""
        try:
            data = c.recv(64)
        except OSError:
            pass
        c.close()
        emit({"ev": "tcp", "peer": norm(a[0]), "port": port, "nonce": nonce(data)})
def udp(port):
    s = bind(socket.SOCK_DGRAM, port)
    while True:
        data, a = s.recvfrom(256)
        emit({"ev": "udp", "peer": norm(a[0]), "port": port, "nonce": nonce(data)})
def v4(h):
    return ".".join(str(int(h[i:i + 2], 16)) for i in (6, 4, 2, 0))
def v6(h):
    words = [h[i:i + 8] for i in range(0, 32, 8)]
    raw = b"".join(bytes.fromhex(w)[::-1] for w in words)
    return norm(socket.inet_ntop(socket.AF_INET6, raw))
def sockets(path, ports, states, proto):
    found = []
    with open(path) as f:
        next(f)
        for line in f:
            p = line.split()
            lh, lp = p[1].split(":")
            rh, rp = p[2].split(":")
            if int(lp, 16) in ports and p[3] in states and int(rp, 16):
                peer = v4(rh) if len(rh) == 8 else v6(rh)
                found.append((peer, int(lp, 16), p[3], proto))
    return found
def overflow():
    with open("/proc/net/netstat") as f:
        rows = [line.split() for line in f if line.startswith("TcpExt:")]
    m = dict(zip(rows[0][1:], rows[1][1:]))
    return int(m.get("ListenOverflows", 0)) + int(m.get("ListenDrops", 0))
def read_tables():
    now = set()
    for path in ("/proc/net/tcp", "/proc/net/tcp6"):
        now.update(sockets(path, TCP, ("01", "03"), "tcp"))
    for path in ("/proc/net/udp", "/proc/net/udp6"):
        now.update(sockets(path, UDP, ("01", "07"), "udp"))
    return now, overflow()
class Sampler:
    # A tick reports sockets true only if every sample since the last tick read
    # the tables, so a gap inside a tick is never reported as sampled.
    def __init__(self, read):
        self.read, self.seen, self.last, self.ok, self.count = read, set(), None, True, None
    def poll(self, now_s):
        docs = []
        try:
            now, self.count = self.read()
        except (OSError, ValueError, IndexError, StopIteration):
            now, self.ok, self.count = set(), False, None
        for peer, port, state, proto in sorted(now - self.seen):
            docs.append({"ev": "sock", "peer": peer, "port": port, "state": state, "proto": proto})
        self.seen |= now
        if self.last is None or now_s - self.last >= 0.5:
            self.last = now_s
            docs.append({"ev": "tick", "sockets": self.ok and self.count is not None,
                         "overflow": self.count})
            self.ok = True
        return docs
if __name__ == "__main__":
    for port in TCP:
        threading.Thread(target=tcp, args=(port,), daemon=True).start()
    for port in UDP:
        threading.Thread(target=udp, args=(port,), daemon=True).start()
    time.sleep(0.2)
    emit({"ev": "ready", "tcp": TCP, "udp": UDP})
    sampler = Sampler(read_tables)
    while True:
        for doc in sampler.poll(time.time()):
            emit(doc)
        time.sleep(0.1)
"""

SENDER_PROGRAM = r"""
import json, os, random, socket, struct, sys, threading, time
cfg = json.loads(sys.argv[1])
timeout = cfg["timeout_ms"] / 1000.0
results = []
lock = threading.Lock()
def dns_query():
    qid = random.randrange(65536)
    name = b"".join(bytes([len(p)]) + p for p in (b"example", b"com")) + b"\x00"
    return struct.pack(">HHHHHH", qid, 0x0100, 1, 0, 0, 0) + name + struct.pack(">HH", 1, 1)
def classify(exc):
    if isinstance(exc, socket.timeout):
        return "timeout"
    if isinstance(exc, ConnectionRefusedError):
        return "refused"
    if isinstance(exc, OSError) and exc.errno in (101, 113, 65, 51):
        return "unreachable"
    return "error"
def attempt(item):
    family = socket.AF_INET6 if ":" in item["addr"] else socket.AF_INET
    proto = item["proto"]
    kind = socket.SOCK_STREAM if proto in ("tcp", "dns_tcp") else socket.SOCK_DGRAM
    start = int(time.time() * 1000)
    s = socket.socket(family, kind)
    s.settimeout(timeout)
    try:
        s.connect((item["addr"], item["port"]))
        if proto == "tcp":
            s.sendall(item["nonce"].encode("ascii"))
            result = "connected"
        elif proto == "dns_tcp":
            q = dns_query()
            s.sendall(struct.pack(">H", len(q)) + q)
            result = "connected"
        else:
            s.send(dns_query() if proto == "dns_udp" else item["nonce"].encode("ascii"))
            s.recv(512)
            result = "reply"
    except Exception as exc:
        result = classify(exc)
    finally:
        s.close()
    with lock:
        results.append(dict(item, result=result, start=start))
begin = int(time.time() * 1000)
for phase in cfg["phases"]:
    threads = []
    for item in phase:
        t = threading.Thread(target=attempt, args=(item,))
        t.start()
        threads.append(t)
        time.sleep(cfg["interval_ms"] / 1000.0)
    for t in threads:
        t.join()
doc = {"ev": "result", "start": begin, "end": int(time.time() * 1000), "attempts": results}
sys.stdout.write(json.dumps(doc, sort_keys=True) + "\n")
sys.stdout.flush()
"""

# Template name -> (program, namespace role). The bootstrap installs these and
# the guard admits probe pods only from them (a later slice installs both).
TEMPLATES: dict[str, tuple[str, str]] = {
    "twin": ("listener", "protected"),
    "allowed_sender": ("sender", "protected"),
    "protected_probe": ("sender", "protected"),
    "runner_policy": ("sender", "protected"),
    "probe_listener": ("listener", "probe"),
    "probe_sender": ("sender", "probe"),
    "release_probe": ("sender", "release"),
}
_PROGRAMS = {"listener": LISTENER_PROGRAM, "sender": SENDER_PROGRAM}
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_NODE = re.compile(r"[a-z0-9][a-z0-9.-]{0,252}")


def probe_kit_digest() -> str:
    """sha256 over the probe image and both programs, recorded with the evidence."""
    return hashlib.sha256(
        canonical_json(
            {"image": PROBE_IMAGE, "listener": LISTENER_PROGRAM, "sender": SENDER_PROGRAM}
        )
    ).hexdigest()


def probe_pod(
    *,
    template: str,
    name: str,
    namespace: str,
    node: str,
    campaign: str,
    role_labels: Mapping[str, str],
    argument: Mapping[str, Any],
) -> dict[str, Any]:
    """Render one probe pod from a template. Only the node, allowed labels and argument vary."""
    require(template in TEMPLATES)
    require(bool(_NAME.fullmatch(name)) and bool(_NAME.fullmatch(namespace)))
    require(bool(_NAME.fullmatch(campaign)) and bool(_NODE.fullmatch(node)))
    require(all(key not in (TEMPLATE_LABEL, PROBE_LABEL, CAMPAIGN_LABEL) for key in role_labels))
    program_kind = TEMPLATES[template][0]
    labels = {
        **dict(role_labels),
        TEMPLATE_LABEL: template,
        PROBE_LABEL: name,
        CAMPAIGN_LABEL: campaign,
    }
    pod_security: dict[str, Any] = {
        "runAsNonRoot": True,
        "runAsUser": 65534,
        "runAsGroup": 65534,
        "seccompProfile": {"type": "RuntimeDefault"},
    }
    if program_kind == "listener":
        # A namespaced safe sysctl, so a twin can hold a target's low port unprivileged.
        pod_security["sysctls"] = [{"name": "net.ipv4.ip_unprivileged_port_start", "value": "0"}]
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": namespace, "labels": labels},
        "spec": {
            "nodeName": node,
            "restartPolicy": "Never",
            "automountServiceAccountToken": False,
            "enableServiceLinks": False,
            "hostNetwork": False,
            "hostPID": False,
            "hostIPC": False,
            "terminationGracePeriodSeconds": 1,
            "securityContext": pod_security,
            "containers": [
                {
                    "name": "probe",
                    "image": PROBE_IMAGE,
                    "imagePullPolicy": "IfNotPresent",
                    "command": [
                        "python3",
                        "-I",
                        "-c",
                        _PROGRAMS[program_kind],
                        json.dumps(argument, sort_keys=True),
                    ],
                    "env": [{"name": "PYTHONDONTWRITEBYTECODE", "value": "1"}],
                    "resources": {
                        "requests": {"cpu": "20m", "memory": "32Mi"},
                        "limits": {"cpu": "200m", "memory": "96Mi"},
                    },
                    "securityContext": {
                        "privileged": False,
                        "allowPrivilegeEscalation": False,
                        "readOnlyRootFilesystem": True,
                        "capabilities": {"drop": ["ALL"]},
                    },
                }
            ],
        },
    }


@dataclass(frozen=True, slots=True)
class ListenerRecord:
    ready: bool
    # (event kind, peer, port, nonce or socket state, wall time ms)
    events: tuple[tuple[str, str, int, str, int], ...]
    # (wall time ms, sockets sampled, overflow counter)
    ticks: tuple[tuple[int, bool, int | None], ...]

    def deliveries(
        self,
        *,
        nonces: Sequence[str],
        peers: Sequence[str],
        start_ms: int,
        end_ms: int,
        foreign_nonces: Set[str] = frozenset(),
    ) -> int:
        """Events or socket entries for any of the probe's nonces or source addresses.

        A nonce is the probe's own at any time. A source address is the probe's
        only inside its own window: after the sender ends, another pod may hold
        it. An event carrying another probe's nonce belongs to that probe.
        """
        wanted_nonces, wanted_peers = set(nonces), {_canonical_ip(peer) for peer in peers}
        count = 0
        for kind, peer, _port, detail, at in self.events:
            payload = kind in ("tcp", "udp")
            if payload and detail in wanted_nonces:
                count += 1
            elif (
                peer in wanted_peers
                and start_ms <= at <= end_ms
                and not (payload and detail in foreign_nonces)
            ):
                count += 1
        return count

    def window(self, start_ms: int, end_ms: int) -> tuple[bool, bool, bool]:
        """(sampled throughout, covers the window, overflow moved) for [start, end]."""
        inside = [tick for tick in self.ticks if start_ms <= tick[0] <= end_ms]
        before = [tick for tick in self.ticks if tick[0] < start_ms]
        after = [tick for tick in self.ticks if tick[0] > end_ms]
        covered = bool(before) and bool(after)
        span = before[-1:] + inside + after[:1]
        sampled = covered and all(tick[1] and tick[2] is not None for tick in span)
        moved = sampled and len({tick[2] for tick in span}) != 1
        return sampled, covered, moved


@dataclass(frozen=True, slots=True)
class SenderRecord:
    start_ms: int
    end_ms: int
    # (address, port, protocol, nonce, result)
    attempts: tuple[tuple[str, int, str, str, str], ...]


def _canonical_ip(value: Any) -> str:
    require(type(value) is str)
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        refuse()
    return str(address)


def _lines(log: str) -> list[dict[str, Any]]:
    require(type(log) is str and len(log) <= LOG_CAP)
    documents = []
    for line in log.splitlines():
        if not line.startswith("{"):
            continue
        try:
            document = json.loads(line)
        except ValueError:
            refuse()
        require(type(document) is dict)
        documents.append(document)
    return documents


def parse_listener_log(log: str) -> ListenerRecord:
    ready = False
    events: list[tuple[str, str, int, str, int]] = []
    ticks: list[tuple[int, bool, int | None]] = []
    for document in _lines(log):
        kind = document.get("ev")
        if kind == "ready":
            ready = True
        elif kind in ("tcp", "udp"):
            events.append(
                (
                    kind,
                    _canonical_ip(document["peer"]),
                    int(document["port"]),
                    str(document["nonce"]),
                    int(document["t"]),
                )
            )
        elif kind == "sock":
            events.append(
                (
                    "sock",
                    _canonical_ip(document["peer"]),
                    int(document["port"]),
                    str(document["state"]),
                    int(document["t"]),
                )
            )
        elif kind == "tick":
            overflow = document["overflow"]
            require(overflow is None or type(overflow) is int)
            ticks.append((int(document["t"]), document["sockets"] is True, overflow))
        else:
            refuse()
    return ListenerRecord(ready=ready, events=tuple(events), ticks=tuple(ticks))


SENDER_RESULTS = frozenset({"connected", "reply", "refused", "timeout", "unreachable", "error"})


def parse_sender_log(log: str) -> SenderRecord:
    documents = [document for document in _lines(log) if document.get("ev") == "result"]
    require(len(documents) == 1)
    document = documents[0]
    attempts = []
    for item in document["attempts"]:
        require(item["result"] in SENDER_RESULTS)
        attempts.append(
            (
                _canonical_ip(item["addr"]),
                int(item["port"]),
                str(item["proto"]),
                str(item["nonce"]),
                str(item["result"]),
            )
        )
    return SenderRecord(
        start_ms=int(document["start"]), end_ms=int(document["end"]), attempts=tuple(attempts)
    )
