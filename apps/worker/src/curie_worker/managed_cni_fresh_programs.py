"""Closed v2 fresh-connection probe logs; parsing alone is not CNI proof.

@spec CNI-FRESH-FLOW-3 @spec CNI-FRESH-FLOW-4. The measured case must join
these values with actual owned Pod identities and an independently minted
post-trigger challenge. Legacy source-address-only logs are not accepted.
"""

from __future__ import annotations

import ipaddress
import json
import re
from dataclasses import dataclass
from typing import Any

from .managed_cni_probe_kit import refuse, require

_HEX = re.compile(r"[0-9a-f]{32}")


def _integer(value: Any, low: int, high: int) -> int:
    require(type(value) is int and low <= value <= high)
    return int(value)


def _address(value: Any) -> str:
    require(type(value) is str)
    try:
        parsed = ipaddress.ip_address(value)
    except ValueError:
        refuse()
    if isinstance(parsed, ipaddress.IPv6Address) and parsed.ipv4_mapped:
        return str(parsed.ipv4_mapped)
    return str(parsed)


def _members(value: Any, names: str) -> dict[str, Any]:
    require(type(value) is dict and set(value) == set(names.split()))
    return dict(value)


def _object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for key, value in pairs:
        require(key not in out)
        out[key] = value
    return out


def _rows(log: str) -> list[dict[str, Any]]:
    require(type(log) is str)
    try:
        size = len(log.encode("utf-8"))
    except UnicodeError:
        refuse()
    require(0 < size <= 8 * 1024 * 1024)
    out: list[dict[str, Any]] = []
    try:
        for line in log.splitlines():
            require(bool(line) and len(out) < 32768)
            row = json.loads(line, object_pairs_hook=_object)
            require(type(row) is dict and row.get("v") == 2 and type(row.get("v")) is int)
            require(type(row.get("seq")) is int and row["seq"] == len(out))
            require(type(row.get("ev")) is str)
            out.append({k: v for k, v in row.items() if k not in ("v", "seq")})
    except (ValueError, TypeError, RecursionError):
        refuse()
    return out


@dataclass(frozen=True, slots=True)
class FreshAttempt:
    target: str
    source: str
    source_port: int
    destination: str
    port: int
    nonce: str
    outcome: str
    echo: bool

    @property
    def endpoint(self) -> tuple[str, int, str, int]:
        return self.source, self.source_port, self.destination, self.port


def _attempt(row: dict[str, Any]) -> FreshAttempt:
    row = _members(row, "ev target source source_port destination port nonce outcome echo")
    require(row["ev"] == "attempt" and row["target"] in ("real", "twin"))
    require(type(row["nonce"]) is str and re.fullmatch(r"[0-9a-f]{36}", row["nonce"]) is not None)
    require(row["outcome"] in ("connected", "timeout", "refused", "error"))
    require(type(row["echo"]) is bool)
    require(not row["echo"] or row["outcome"] == "connected")
    return FreshAttempt(
        row["target"],
        _address(row["source"]),
        _integer(row["source_port"], 1, 65535),
        _address(row["destination"]),
        _integer(row["port"], 1, 65535),
        row["nonce"],
        row["outcome"],
        row["echo"],
    )


@dataclass(frozen=True, slots=True)
class FreshSenderRecord:
    mode: str
    source: str
    setup: FreshAttempt | None
    challenge: str | None
    attempts: tuple[FreshAttempt, ...]
    complete: bool
    refusal: str | None


def parse_fresh_sender_log(log: str, *, allow_partial: bool = False) -> FreshSenderRecord:
    rows = _rows(log)
    ready = _members(rows[0], "ev mode source setup targets excluded_ports")
    require(ready["ev"] == "ready" and ready["mode"] in ("deny", "control", "setup"))
    source = _address(ready["source"])
    targets = ready["targets"]
    require(type(targets) is list and 1 <= len(targets) <= 2)
    names: set[str] = set()
    endpoints: set[tuple[str, int]] = set()
    for target in targets:
        _members(target, "target address port echo")
        require(target["target"] in ("real", "twin") and target["target"] not in names)
        names.add(target["target"])
        endpoint = _address(target["address"]), _integer(target["port"], 1, 65535)
        require(endpoint not in endpoints and endpoint[1] != 42001)
        require(type(target["echo"]) is bool and target["echo"] == (target["target"] == "twin"))
        require(ipaddress.ip_address(endpoint[0]).version == ipaddress.ip_address(source).version)
        endpoints.add(endpoint)
    excluded = ready["excluded_ports"]
    require(type(excluded) is list and len(excluded) <= 64)
    require(len(set(_integer(p, 1, 65535) for p in excluded)) == len(excluded))
    setup = None if ready["setup"] is None else _attempt(ready["setup"])
    if setup is not None:
        require(setup.source == source and setup.outcome == "connected")
        require(setup.source_port in excluded)
        require(
            (setup.target, setup.destination, setup.port)
            == (targets[0]["target"], _address(targets[0]["address"]), targets[0]["port"])
        )
        require(not targets[0]["echo"] or setup.echo)
    challenge: str | None = None
    attempts: list[FreshAttempt] = []
    used: set[int] = set(excluded)
    complete, terminal, refusal = False, False, None
    for row in rows[1:]:
        require(not terminal)
        if row["ev"] == "challenge":
            _members(row, "ev challenge")
            require(challenge is None and not attempts and ready["mode"] != "setup")
            require(type(row["challenge"]) is str and _HEX.fullmatch(row["challenge"]) is not None)
            challenge = row["challenge"]
        elif row["ev"] == "attempt":
            if challenge is None or refusal is not None:
                refuse()
            attempt = _attempt(row)
            count = 5 if ready["mode"] == "deny" else 1
            index, ordinal = divmod(len(attempts), count)
            require(index < len(targets))
            target = targets[index]
            require(
                (attempt.target, attempt.destination, attempt.port)
                == (target["target"], _address(target["address"]), target["port"])
            )
            require(attempt.source == source and attempt.source_port not in used)
            require(attempt.nonce == challenge + f"{index:02x}{ordinal:02x}")
            require(not attempt.echo or target["echo"])
            used.add(attempt.source_port)
            attempts.append(attempt)
        elif row["ev"] == "refused":
            _members(row, "ev reason")
            require(
                refusal is None
                and row["reason"] in ("barrier_invalid", "transport_error", "ports_unavailable")
            )
            refusal = row["reason"]
        elif row["ev"] == "done":
            _members(row, "ev ok")
            require(type(row["ok"]) is bool)
            terminal = True
            if row["ok"]:
                require(refusal is None and challenge is not None)
                require(len(attempts) == len(targets) * (5 if ready["mode"] == "deny" else 1))
                complete = True
            else:
                require(refusal is not None)
        else:
            refuse()
    require(allow_partial or terminal)
    return FreshSenderRecord(
        ready["mode"], source, setup, challenge, tuple(attempts), complete, refusal
    )


@dataclass(frozen=True, slots=True)
class FreshListenerRecord:
    port: int
    events: tuple[tuple[str, tuple[str, int, str, int], str | None, int], ...]
    ticks: tuple[tuple[int, bool, int | None, int], ...]

    def deliveries(self, attempts: tuple[FreshAttempt, ...]) -> int:
        endpoints = {a.endpoint for a in attempts if a.target == "twin"}
        nonces = {a.nonce for a in attempts if a.target == "twin"}
        return sum(
            endpoint in endpoints or (nonce is not None and nonce in nonces)
            for _kind, endpoint, nonce, _seq in self.events
        )

    def covers(self, control: FreshAttempt) -> bool:
        """Sampling bracket only; caller separately proves the actual control."""
        data = [
            seq
            for kind, endpoint, nonce, seq in self.events
            if kind == "data" and endpoint == control.endpoint and nonce == control.nonce
        ]
        if not data:
            return False
        after = next((tick for tick in self.ticks if tick[3] > data[0]), None)
        if after is None:
            return False
        ticks = [tick for tick in self.ticks if tick[3] <= after[3]]
        if len(ticks) < 2 or ticks[0][3] >= data[0]:
            return False
        if not all(sampled and overflow is not None for _ms, sampled, overflow, _seq in ticks):
            return False
        return len({tick[2] for tick in ticks}) == 1 and all(
            right[0] - left[0] <= 1000 for left, right in zip(ticks, ticks[1:], strict=False)
        )


def parse_fresh_listener_log(log: str) -> FreshListenerRecord:
    rows = _rows(log)
    first = _members(rows[0], "ev port")
    require(first["ev"] == "ready")
    port = _integer(first["port"], 1, 65535)
    events = []
    ticks = []
    last = -1
    for ordinal, row in enumerate(rows[1:], 1):
        if row["ev"] == "tick":
            _members(row, "ev elapsed_ms sampled overflow")
            elapsed = _integer(row["elapsed_ms"], 0, 2**63 - 1)
            require(elapsed > last and type(row["sampled"]) is bool)
            last = elapsed
            overflow = None if row["overflow"] is None else _integer(row["overflow"], 0, 2**63 - 1)
            require(not row["sampled"] or overflow is not None)
            ticks.append((elapsed, row["sampled"], overflow, ordinal))
            continue
        names = "ev source source_port destination port"
        require(row["ev"] in ("accept", "data", "socket"))
        if row["ev"] == "data":
            names += " nonce"
            require(
                type(row.get("nonce")) is str
                and re.fullmatch(r"[0-9a-f]{32,64}", row["nonce"]) is not None
            )
        if row["ev"] == "socket":
            names += " state"
            require(row.get("state") in ("01", "03"))
        _members(row, names)
        require(_integer(row["port"], 1, 65535) == port)
        endpoint = (
            _address(row["source"]),
            _integer(row["source_port"], 1, 65535),
            _address(row["destination"]),
            port,
        )
        events.append((row["ev"], endpoint, row.get("nonce"), ordinal))
    return FreshListenerRecord(port, tuple(events), tuple(ticks))


@dataclass(frozen=True, slots=True)
class FreshTriggerRecord:
    challenge: str
    acknowledged: bool


def parse_fresh_trigger_log(log: str) -> FreshTriggerRecord:
    rows = _rows(log)
    require(len(rows) == 2)
    row = _members(rows[0], "ev challenge acknowledged")
    require(row["ev"] == "trigger" and type(row["challenge"]) is str)
    require(_HEX.fullmatch(row["challenge"]) is not None and type(row["acknowledged"]) is bool)
    done = _members(rows[1], "ev ok")
    require(done["ev"] == "done" and type(done["ok"]) is bool and done["ok"] == row["acknowledged"])
    return FreshTriggerRecord(row["challenge"], row["acknowledged"])


# Fixed stdlib-only programs. They run inside owned probe Pods, never as an
# operator command executor. Their complete bytes participate in the kit digest.
_RUNTIME = r"""
import ipaddress, json, re, socket, sys, threading, time

seq = 0
output_lock = threading.Lock()


def emit(row):
    global seq
    with output_lock:
        sys.stdout.write(json.dumps({"v": 2, "seq": seq, **row}, sort_keys=True) + "\n")
        sys.stdout.flush()
        seq += 1


def pairs(rows):
    value = {}
    for k, v in rows:
        if k in value:
            raise ValueError("duplicate")
        value[k] = v
    return value


def keys(value, names):
    if type(value) is not dict or set(value) != set(names.split()):
        raise ValueError("shape")


def integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ValueError("integer")
    return value


def norm(value):
    addr = ipaddress.ip_address(value)
    return str(
        addr.ipv4_mapped if isinstance(addr, ipaddress.IPv6Address) and addr.ipv4_mapped else addr
    )


def hex32(value):
    if type(value) is not str or re.fullmatch("[0-9a-f]{32}", value) is None:
        raise ValueError("challenge")
    return value


def family(value):
    if value not in ("ipv4", "ipv6"):
        raise ValueError("family")
    return socket.AF_INET if value == "ipv4" else socket.AF_INET6


def address(value, af):
    if type(value) is not str:
        raise ValueError("address")
    result = norm(value)
    if ipaddress.ip_address(result).version != (4 if af == socket.AF_INET else 6):
        raise ValueError("family")
    return result


def receive(sock, cap):
    data = b""
    while len(data) < cap:
        part = sock.recv(cap - len(data))
        if not part:
            return data
        data += part
    raise ValueError("frame")


cfg = json.loads(sys.argv[1], object_pairs_hook=pairs)
"""

FRESH_SENDER_PROGRAM = (
    _RUNTIME
    + r"""
mode = cfg.get("mode")
expected = "mode family targets excluded_ports setup"
if cfg.get("setup") is True:
    expected += " setup_nonce"
if mode == "control":
    expected += " challenge"
keys(cfg, expected)
if mode not in ("setup", "deny", "control") or type(cfg["setup"]) is not bool:
    raise ValueError("mode")
if mode == "setup" and not cfg["setup"]:
    raise ValueError("setup")
if mode == "control" and cfg["setup"]:
    raise ValueError("setup")
af = family(cfg["family"])
targets = cfg["targets"]
if type(targets) is not list or not 1 <= len(targets) <= 2:
    raise ValueError("targets")
names, destinations = set(), set()
for target in targets:
    keys(target, "target address port echo")
    if target["target"] not in ("real", "twin") or target["target"] in names:
        raise ValueError("target")
    names.add(target["target"])
    target["address"] = address(target["address"], af)
    integer(target["port"], 1, 65535)
    if type(target["echo"]) is not bool or target["echo"] != (target["target"] == "twin"):
        raise ValueError("echo")
    endpoint = (target["address"], target["port"])
    if endpoint in destinations or target["port"] == 42001:
        raise ValueError("endpoint")
    destinations.add(endpoint)
excluded = cfg["excluded_ports"]
if type(excluded) is not list or len(excluded) > 63:
    raise ValueError("excluded")
used = set(integer(p, 1, 65535) for p in excluded)
if len(used) != len(excluded):
    raise ValueError("excluded")
# UDP connect selects the local routing address; no datagram is sent.
with socket.socket(af, socket.SOCK_DGRAM) as route:
    route.connect((targets[0]["address"], targets[0]["port"]))
    source = norm(route.getsockname()[0])


def allocate():
    for _ in range(64):
        sock = socket.socket(af, socket.SOCK_STREAM)
        sock.settimeout(2.0)
        sock.bind((source, 0))
        sport = sock.getsockname()[1]
        if sport not in used:
            used.add(sport)
            return sock, sport
        sock.close()
    raise ValueError("ports")


def attempt(target, nonce, hold=False):
    sock, sport = allocate()
    result = {
        "ev": "attempt",
        "target": target["target"],
        "source": source,
        "source_port": sport,
        "destination": target["address"],
        "port": target["port"],
        "nonce": nonce,
        "outcome": "error",
        "echo": False,
    }
    try:
        sock.connect((target["address"], target["port"]))
        result["outcome"] = "connected"
        if target["echo"]:
            sock.sendall(nonce.encode("ascii") + b"\n")
            got = b""
            while len(got) <= 64 and not got.endswith(b"\n"):
                part = sock.recv(65 - len(got))
                if not part:
                    break
                got += part
            result["echo"] = got == nonce.encode("ascii") + b"\n"
    except ConnectionRefusedError:
        result["outcome"] = "refused"
    except (TimeoutError, socket.timeout):
        if result["outcome"] != "connected":
            result["outcome"] = "timeout"
    except OSError:
        pass
    finally:
        if not hold or result["outcome"] != "connected":
            sock.close()
    return result, sock


held, setup = None, None
if cfg["setup"]:
    setup, held = attempt(targets[0], hex32(cfg["setup_nonce"]) + "ffff", True)
    if setup["outcome"] != "connected" or (targets[0]["echo"] and not setup["echo"]):
        raise ValueError("setup")
server = None
if mode == "deny":
    server = socket.socket(af, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    if af == socket.AF_INET6:
        server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
    server.bind(("0.0.0.0" if af == socket.AF_INET else "::", 42001))
    server.listen(1)
    server.settimeout(120.0)
emit(
    {
        "ev": "ready",
        "mode": mode,
        "source": source,
        "setup": setup,
        "targets": targets,
        "excluded_ports": sorted(used),
    }
)
try:
    if mode == "setup":
        time.sleep(120.0)
        raise TimeoutError("setup expired")
    if mode == "deny":
        with server:
            conn, _peer = server.accept()
            with conn:
                conn.settimeout(2.0)
                try:
                    message = receive(conn, 40)
                    if (
                        len(message) != 39
                        or not message.startswith(b"START ")
                        or not message.endswith(b"\n")
                    ):
                        raise ValueError("barrier")
                    challenge = hex32(message[6:-1].decode("ascii"))
                except (OSError, ValueError, UnicodeError):
                    emit({"ev": "refused", "reason": "barrier_invalid"})
                    emit({"ev": "done", "ok": False})
                    sys.exit(2)
                emit({"ev": "challenge", "challenge": challenge})
                conn.sendall(b"ACK " + challenge.encode("ascii") + b"\n")
    else:
        challenge = hex32(cfg["challenge"])
        emit({"ev": "challenge", "challenge": challenge})
    count = 5 if mode == "deny" else 1
    for index, target in enumerate(targets):
        for ordinal in range(count):
            row, _closed = attempt(target, challenge + f"{index:02x}{ordinal:02x}")
            emit(row)
            if ordinal + 1 < count:
                time.sleep(0.2)
    emit({"ev": "done", "ok": True})
except (OSError, ValueError):
    emit({"ev": "refused", "reason": "transport_error"})
    emit({"ev": "done", "ok": False})
    sys.exit(2)
finally:
    if held is not None:
        held.close()
"""
)

FRESH_TRIGGER_PROGRAM = (
    _RUNTIME
    + r"""
keys(cfg, "family address challenge")
af = family(cfg["family"])
target, challenge = address(cfg["address"], af), hex32(cfg["challenge"])
ack = False
try:
    with socket.socket(af, socket.SOCK_STREAM) as sock:
        sock.settimeout(2.0)
        sock.connect((target, 42001))
        sock.sendall(b"START " + challenge.encode("ascii") + b"\n")
        sock.shutdown(socket.SHUT_WR)
        ack = receive(sock, 38) == b"ACK " + challenge.encode("ascii") + b"\n"
except (OSError, ValueError):
    pass
emit({"ev": "trigger", "challenge": challenge, "acknowledged": ack})
emit({"ev": "done", "ok": ack})
sys.exit(0 if ack else 2)
"""
)

FRESH_LISTENER_PROGRAM = (
    _RUNTIME
    + r"""
keys(cfg, "family port")
af, port = family(cfg["family"]), integer(cfg["port"], 1, 65535)
if port == 42001:
    raise ValueError("barrier overlap")
server = socket.socket(af, socket.SOCK_STREAM)
server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
if af == socket.AF_INET6:
    server.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 1)
server.bind(("0.0.0.0" if af == socket.AF_INET else "::", port))
server.listen(64)


def serve(conn, peer):
    local = conn.getsockname()
    endpoint = {
        "source": norm(peer[0]),
        "source_port": peer[1],
        "destination": norm(local[0]),
        "port": local[1],
    }
    emit({"ev": "accept", **endpoint})
    conn.settimeout(120.0)
    pending = b""
    try:
        while True:
            data = conn.recv(128)
            if not data:
                break
            pending += data
            if len(pending) > 128:
                break
            while b"\n" in pending:
                row, pending = pending.split(b"\n", 1)
                if re.fullmatch(b"[0-9a-f]{32,64}", row) is None:
                    return
                emit({"ev": "data", **endpoint, "nonce": row.decode("ascii")})
                conn.sendall(row + b"\n")
    except OSError:
        pass
    finally:
        conn.close()


def accept():
    while True:
        conn, peer = server.accept()
        threading.Thread(target=serve, args=(conn, peer), daemon=True).start()


def decode(hexaddr):
    if len(hexaddr) == 8:
        return norm(socket.inet_ntop(socket.AF_INET, bytes.fromhex(hexaddr)[::-1]))
    raw = b"".join(bytes.fromhex(hexaddr[i : i + 8])[::-1] for i in range(0, 32, 8))
    return norm(socket.inet_ntop(socket.AF_INET6, raw))


def sample():
    found = set()
    with open("/proc/net/tcp" if af == socket.AF_INET else "/proc/net/tcp6") as table:
        next(table)
        for line in table:
            fields = line.split()
            local, remote = fields[1].split(":"), fields[2].split(":")
            if int(local[1], 16) == port and fields[3] in ("01", "03"):
                found.add(
                    (decode(remote[0]), int(remote[1], 16), decode(local[0]), port, fields[3])
                )
    with open("/proc/net/netstat") as stream:
        rows = [line.split()[1:] for line in stream if line.startswith("TcpExt:")]
    values = dict(zip(rows[0], rows[1]))
    overflow = int(values["ListenOverflows"]) + int(values["ListenDrops"])
    return found, overflow


emit({"ev": "ready", "port": port})
threading.Thread(target=accept, daemon=True).start()
start = time.monotonic()
last_poll, last_tick, good, over, seen = start, -1.0, True, None, set()
while True:
    now = time.monotonic()
    if now - last_poll > 1.0:
        good = False
    last_poll = now
    try:
        current, over = sample()
        for source, sport, destination, dport, state in sorted(current - seen):
            emit(
                {
                    "ev": "socket",
                    "source": source,
                    "source_port": sport,
                    "destination": destination,
                    "port": dport,
                    "state": state,
                }
            )
        seen |= current
    except (OSError, ValueError, IndexError, KeyError, StopIteration):
        good, over = False, None
    if last_tick < 0 or now - last_tick >= 0.5:
        last_tick = now
        emit(
            {
                "ev": "tick",
                "elapsed_ms": int((now - start) * 1000),
                "sampled": good,
                "overflow": over,
            }
        )
        good = True
    time.sleep(0.1)
"""
)


FRESH_TEMPLATES = {
    "fresh_twin": ("fresh_listener", "protected"),
    "fresh_probe_listener": ("fresh_listener", "probe"),
    "fresh_setup": ("fresh_sender", "protected"),
    "fresh_sender": ("fresh_sender", "probe"),
    "fresh_control": ("fresh_sender", "probe"),
    "fresh_allowed_control": ("fresh_sender", "protected"),
    "fresh_trigger": ("fresh_trigger", "probe"),
}
FRESH_PROGRAMS = {
    "fresh_listener": FRESH_LISTENER_PROGRAM,
    "fresh_sender": FRESH_SENDER_PROGRAM,
    "fresh_trigger": FRESH_TRIGGER_PROGRAM,
}


def validate_fresh_argument(template: str, argument: dict[str, Any]) -> None:
    """Refuse malformed or widening arguments before rendering a Pod."""
    require(template in FRESH_TEMPLATES)
    program = FRESH_TEMPLATES[template][0]
    require(argument.get("family") in ("ipv4", "ipv6"))
    version = 4 if argument["family"] == "ipv4" else 6

    def address(value: Any) -> str:
        result = _address(value)
        require(ipaddress.ip_address(result).version == version)
        return result

    def challenge(value: Any) -> None:
        require(type(value) is str and _HEX.fullmatch(value) is not None)

    if program == "fresh_listener":
        _members(argument, "family port")
        require(_integer(argument["port"], 1, 65535) != 42001)
        return
    if program == "fresh_trigger":
        _members(argument, "family address challenge")
        address(argument["address"])
        challenge(argument["challenge"])
        return
    mode = {
        "fresh_setup": "setup",
        "fresh_sender": "deny",
        "fresh_control": "control",
        "fresh_allowed_control": "control",
    }[template]
    names = "mode family targets excluded_ports setup"
    if argument.get("setup") is True:
        names += " setup_nonce"
    if mode == "control":
        names += " challenge"
        challenge(argument.get("challenge"))
    _members(argument, names)
    require(argument["mode"] == mode and type(argument["setup"]) is bool)
    if mode == "setup":
        require(argument["setup"])
    if mode == "control":
        require(not argument["setup"])
    if argument["setup"]:
        challenge(argument["setup_nonce"])
    targets = argument["targets"]
    require(type(targets) is list and 1 <= len(targets) <= 2)
    seen: set[str] = set()
    endpoints: set[tuple[str, int]] = set()
    for target in targets:
        _members(target, "target address port echo")
        require(target["target"] in ("real", "twin") and target["target"] not in seen)
        seen.add(target["target"])
        require(type(target["echo"]) is bool and target["echo"] == (target["target"] == "twin"))
        endpoint = address(target["address"]), _integer(target["port"], 1, 65535)
        require(endpoint[1] != 42001 and endpoint not in endpoints)
        endpoints.add(endpoint)
    excluded = argument["excluded_ports"]
    require(type(excluded) is list and len(excluded) <= 63)
    require(len(set(_integer(port, 1, 65535) for port in excluded)) == len(excluded))
