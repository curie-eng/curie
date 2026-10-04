"""Provisioner permission recipes, @spec PROTECTED-HOOK-ADMISSION-6."""

# @spec PROTECTED-HOOK-ADMISSION-6 PROTECTED-HOOK-LANE-3
_RESET = ("-@all", "resetkeys", "resetchannels", "clearselectors")
_HANDSHAKE = ("+auth", "+hello", "+ping", "+client|setname", "+client|setinfo")
_READS = (
    "%R~protected:source:*",
    "%R~protected:control:*",
    "+get",
    "+type",
    "+info|server",
    "+time",
)
_METADATA_KEYS = (
    "%RW~protected:admission:intent:*",
    "%RW~protected:admission:state:*",
    "%RW~protected:admission:commit:*",
    "%RW~protected:admission:recovery:*",
    "%RW~protected:admission:binding:*",
)
_ADMISSION_KEYS = (*_METADATA_KEYS, "%RW~protected:admission:quota", "%RW~curie:runs")
_ENQUEUE = (
    *_RESET,
    *_HANDSHAKE,
    *_READS,
    "+eval",
    "(+get +type +set +del " + " ".join(_METADATA_KEYS) + ")",
    "(+type +zadd +zcard +zrem +zscore %RW~protected:admission:quota)",
    "(+type +xinfo|stream +xrange +xadd %RW~curie:runs)",
    "(+eval %RW~protected:source:* %RW~protected:control:* " + " ".join(_ADMISSION_KEYS) + ")",
)
_VERIFIER = (*_RESET, *_HANDSHAKE, *_READS, "(+set %W~protected:control:readiness:*)")


def admission_acl_rules(role: str) -> tuple[str, ...]:
    """@spec PROTECTED-HOOK-ADMISSION-6 PROTECTED-HOOK-LANE-3."""
    if type(role) is str:
        if role == "enqueue":
            return _ENQUEUE
        if role == "verifier":
            return _VERIFIER
    raise ValueError("unknown protected admission role")
