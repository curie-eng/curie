"""Provisioner recipe for the protected worker, @spec PROTECTED-HOOK-LANE-3."""

_RESET = ("-@all", "resetkeys", "resetchannels", "clearselectors")
_HANDSHAKE = ("+auth", "+hello", "+ping", "+client|setname", "+client|setinfo")
_READS = (
    "(+get %R~protected:control:* %R~protected:admission:binding:*)",
    "+info|server",
    "+time",
)
# These commands are measured against the unchanged worker and its Lua scripts.
# Keep control/binding reads in their own selector: a global GET grant must not
# give mutating commands authority over immutable admission or control records.
_COMMANDS = (
    "get set del exists expire pexpire pexpireat pexpiretime pttl ttl incr "
    "hget hset hsetnx hdel hgetall hmget hexists hincrby "
    "sadd srem scard smembers sismember srandmember "
    "zadd zrem zscore zcount zrangebyscore multi exec eval evalsha script|load scan "
    "xack xadd xautoclaim xclaim xdel xgroup|create xgroup|delconsumer "
    "xinfo|consumers xinfo|groups xlen xpending xrange xreadgroup xrevrange"
)
_KEYS = (
    "~protected:lane:*",
    "~curie:runs",
    "~curie:runs:consumer-heartbeat:*",
    "~curie:runs:consumer-heartbeat-capable:*",
    "~curie:runs:consumer-reclaim-lock:*",
    # DeliveryLeaseStore declares the empty second key without a resume marker.
    # The empty pattern matches that key only, never a foreign key family.
    "~",
)
_WORKER = (
    *_RESET,
    *_HANDSHAKE,
    *_READS,
    "(" + " ".join("+" + command for command in _COMMANDS.split()) + " " + " ".join(_KEYS) + ")",
)


def worker_acl_rules(role: str) -> tuple[str, ...]:
    """Closed worker authority, @spec PROTECTED-HOOK-LANE-3 PROTECTED-HOOK-LANE-7."""
    if type(role) is str and role == "worker":
        return _WORKER
    raise ValueError("unknown protected worker role")
