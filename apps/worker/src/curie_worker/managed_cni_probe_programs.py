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
HOLD = cfg.get("hold", False)
def serve(c, peer, port):
    # Retained-connection mode (case 5): keep the accepted socket open and log
    # every segment's nonce, so a flow that keeps passing data after a trigger
    # is observed. Default mode closes after one recv, unchanged.
    c.settimeout(30.0)
    try:
        while True:
            data = c.recv(256)
            if not data:
                break
            emit({"ev": "tcp", "peer": peer, "port": port, "nonce": nonce(data)})
    except OSError:
        pass
    finally:
        c.close()
def tcp(port):
    s = bind(socket.SOCK_STREAM, port)
    s.listen(64)
    while True:
        c, a = s.accept()
        if HOLD:
            threading.Thread(target=serve, args=(c, norm(a[0]), port), daemon=True).start()
            continue
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

# Case 3 raw-socket sender. It forges the IP source address, so it never
# receives the reply and only the owned listener decides the verdict. IPv4 only:
# a forged IPv6 source needs L2 crafting that NET_RAW alone does not give, so the
# case code marks any IPv6 spoof entry unavailable rather than widen rights. The
# only capability this adds is NET_RAW, in probe-a (clause 2).
SPOOF_PROGRAM = r"""
import json, os, socket, struct, sys, time
cfg = json.loads(sys.argv[1])
def checksum(data):
    if len(data) % 2:
        data += b"\x00"
    total = 0
    for i in range(0, len(data), 2):
        total += (data[i] << 8) + data[i + 1]
    total = (total >> 16) + (total & 0xffff)
    total += total >> 16
    return (~total) & 0xffff
def ip_header(src, dst, proto, payload_len):
    total = 20 + payload_len
    fields = (0x45, 0, total, os.getpid() & 0xffff, 0, 64, proto, 0,
              socket.inet_aton(src), socket.inet_aton(dst))
    chk = checksum(struct.pack(">BBHHHBBH4s4s", *fields))
    fields = fields[:7] + (chk,) + fields[8:]
    return struct.pack(">BBHHHBBH4s4s", *fields)
def tcp_segment(src, dst, sport, dport, seq, ack, flags, payload):
    base = (sport, dport, seq, ack, 5 << 4, flags, 1024, 0, 0)
    pseudo = socket.inet_aton(src) + socket.inet_aton(dst) + struct.pack(
        ">BBH", 0, 6, 20 + len(payload))
    chk = checksum(pseudo + struct.pack(">HHLLBBHHH", *base) + payload)
    base = base[:7] + (chk, 0)
    return struct.pack(">HHLLBBHHH", *base) + payload
def udp_datagram(src, dst, sport, dport, payload):
    length = 8 + len(payload)
    pseudo = socket.inet_aton(src) + socket.inet_aton(dst) + struct.pack(">BBH", 0, 17, length)
    chk = checksum(pseudo + struct.pack(">HHHH", sport, dport, length, 0) + payload) or 0xffff
    return struct.pack(">HHHH", sport, dport, length, chk) + payload
results = []
for item in cfg["packets"]:
    src, dst, dport = item["src"], item["addr"], item["port"]
    proto, nonce = item["proto"], item["nonce"]
    sport = int(item.get("sport") or (40000 + os.getpid() % 20000))
    if ":" in src or ":" in dst:
        results.append([nonce, "unsupported_family"])
        continue
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_RAW, socket.IPPROTO_RAW)
        s.setsockopt(socket.IPPROTO_IP, socket.IP_HDRINCL, 1)
        if proto == "udp":
            seg = udp_datagram(src, dst, sport, dport, nonce.encode("ascii"))
            packet = ip_header(src, dst, 17, len(seg)) + seg
        else:
            seq = int(item.get("seq") or int.from_bytes(os.urandom(4), "big"))
            ack = int(item.get("ack") or 0)
            flags = int(item.get("flags") or 0x02)  # SYN
            payload = nonce.encode("ascii") if item.get("with_payload") else b""
            seg = tcp_segment(src, dst, sport, dport, seq, ack, flags, payload)
            packet = ip_header(src, dst, 6, len(seg)) + seg
        for _ in range(int(cfg.get("repeat", 1))):
            s.sendto(packet, (dst, 0))
            time.sleep(cfg.get("interval_ms", 0) / 1000.0)
        s.close()
        results.append([nonce, "sent"])
    except Exception as exc:  # the forged send cannot be observed here; the twin decides
        results.append([nonce, "error:" + type(exc).__name__])
sys.stdout.write(json.dumps({"ev": "result", "packets": results,
                             "t": int(time.time() * 1000)}, sort_keys=True) + "\n")
sys.stdout.flush()
"""

# Case 5 retained sender: establish a TCP flow, then send sequence-numbered
# heartbeat nonces at a fixed interval, past the triggering event. Each send is
# logged with its wall time so the kit can tell which heartbeats were sent after
# the trigger plus the bound.
RETAINED_SENDER_PROGRAM = r"""
import json, socket, sys, time
cfg = json.loads(sys.argv[1])
addr, port = cfg["addr"], cfg["port"]
family = socket.AF_INET6 if ":" in addr else socket.AF_INET
s = socket.socket(family, socket.SOCK_STREAM)
s.settimeout(cfg["timeout_ms"] / 1000.0)
out = {"ev": "result", "connected": False, "laddr": None, "lport": None, "sent": []}
def emit(doc):
    doc["t"] = int(time.time() * 1000)
    sys.stdout.write(json.dumps(doc, sort_keys=True) + "\n")
    sys.stdout.flush()
try:
    s.connect((addr, port))
    out["connected"] = True
    out["laddr"], out["lport"] = s.getsockname()[0], s.getsockname()[1]
    emit({"ev": "established", "laddr": out["laddr"], "lport": out["lport"]})
    for nonce in cfg["nonces"]:
        try:
            s.sendall(nonce.encode("ascii"))
            out["sent"].append([nonce, int(time.time() * 1000), "ok"])
        except OSError:
            out["sent"].append([nonce, int(time.time() * 1000), "error"])
        time.sleep(cfg["interval_ms"] / 1000.0)
except Exception:
    pass
finally:
    s.close()
emit(out)
"""

# Case 7 node_origin: an idle pod whose readiness probe makes kubelet connect to
# a twin from the node itself. The container runs no network code; the readiness
# probe the template carries is the only traffic.
IDLE_PROGRAM = r"""
import json, sys, time
sys.stdout.write(json.dumps({"ev": "ready"}) + "\n")
sys.stdout.flush()
while True:
    time.sleep(3600)
"""

# Template name -> (program, namespace role). The bootstrap installs these and
# the guard admits probe pods only from them (a later slice installs both). The
# ``spoofing`` template adds NET_RAW (clause 2); every other template drops all.
TEMPLATES: dict[str, tuple[str, str]] = {
    "twin": ("listener", "protected"),
    "allowed_sender": ("sender", "protected"),
    "protected_probe": ("sender", "protected"),
    "runner_policy": ("sender", "protected"),
    "probe_listener": ("listener", "probe"),
    "probe_sender": ("sender", "probe"),
    "release_probe": ("sender", "release"),
    "spoofing": ("spoof", "probe"),
    "retained_sender": ("retained_sender", "protected"),
    "kubelet_probe": ("idle", "probe"),
}
# Only the spoofing template adds a capability, and only NET_RAW (clause 2).
NET_RAW_TEMPLATES = frozenset({"spoofing"})
_PROGRAMS = {
    "listener": LISTENER_PROGRAM,
    "sender": SENDER_PROGRAM,
    "spoof": SPOOF_PROGRAM,
    "retained_sender": RETAINED_SENDER_PROGRAM,
    "idle": IDLE_PROGRAM,
}
_NAME = re.compile(r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")
_NODE = re.compile(r"[a-z0-9][a-z0-9.-]{0,252}")


def probe_kit_digest() -> str:
    """sha256 over the probe image and every program, recorded with the evidence."""
    return hashlib.sha256(
        canonical_json({"image": PROBE_IMAGE, "programs": dict(sorted(_PROGRAMS.items()))})
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
    if program_kind in ("listener", "retained_sender"):
        # A namespaced safe sysctl, so a twin can hold a target's low port, and a
        # retained sender can bind the original low source port, unprivileged.
        pod_security["sysctls"] = [{"name": "net.ipv4.ip_unprivileged_port_start", "value": "0"}]
    capabilities: dict[str, Any] = {"drop": ["ALL"]}
    if template in NET_RAW_TEMPLATES:
        capabilities["add"] = ["NET_RAW"]
    container: dict[str, Any] = {
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
            "capabilities": capabilities,
        },
    }
    if program_kind == "idle":
        # Case 7: kubelet connects to the twin from the node to run this probe.
        require("probe_host" in argument and "probe_port" in argument)
        host, port = argument["probe_host"], argument["probe_port"]
        require(type(host) is str and type(port) is int and bool(_NODE.fullmatch(host)))
        require(0 < port < 65536)
        container["readinessProbe"] = {
            "tcpSocket": {"host": host, "port": port},
            "initialDelaySeconds": 1,
            "periodSeconds": 2,
            "failureThreshold": 60,
        }
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
            "containers": [container],
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


SPOOF_RESULTS = frozenset({"sent", "unsupported_family"})


@dataclass(frozen=True, slots=True)
class SpoofRecord:
    # (nonce, result): ``sent`` means the forged packet left the raw socket. The
    # listener, not this record, decides whether it was delivered.
    packets: tuple[tuple[str, str], ...]

    @property
    def sent(self) -> bool:
        return bool(self.packets) and all(result == "sent" for _nonce, result in self.packets)


def parse_spoof_log(log: str) -> SpoofRecord:
    documents = [document for document in _lines(log) if document.get("ev") == "result"]
    require(len(documents) == 1)
    packets = []
    for nonce, result in documents[0]["packets"]:
        prefix = result.split(":", 1)[0]
        require(result in SPOOF_RESULTS or prefix == "error")
        packets.append((str(nonce), str(result)))
    return SpoofRecord(packets=tuple(packets))


@dataclass(frozen=True, slots=True)
class RetainedSenderRecord:
    connected: bool
    local_address: str | None
    local_port: int | None
    established_ms: int | None
    # (nonce, send wall time ms, result)
    sent: tuple[tuple[str, int, str], ...]


def parse_retained_sender_log(log: str) -> RetainedSenderRecord:
    established_ms = None
    result: dict[str, Any] | None = None
    for document in _lines(log):
        if document.get("ev") == "established":
            established_ms = int(document["t"])
        elif document.get("ev") == "result":
            result = document
    require(result is not None)
    assert result is not None
    sent = []
    for nonce, at, outcome in result["sent"]:
        require(outcome in ("ok", "error"))
        sent.append((str(nonce), int(at), str(outcome)))
    local_address = None if result["laddr"] is None else _canonical_ip(result["laddr"])
    local_port = None if result["lport"] is None else int(result["lport"])
    return RetainedSenderRecord(
        connected=result["connected"] is True,
        local_address=local_address,
        local_port=local_port,
        established_ms=established_ms,
        sent=tuple(sent),
    )
