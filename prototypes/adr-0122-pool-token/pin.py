"""Does a long-lived adopted thread present the token its pod booted with,
after the source behind that token has rotated underneath it?

The peer's question. Run against shipped code, not a spike.
"""
from curie_worker.sandbox.substrate import SandboxSubstrate
from curie_worker.sandbox.types import RouteRecord, SandboxHandle, RouteState, SubstrateConfig

PINNED, ROTATED = "gen-one-token", "gen-two-token"
calls = []

class Obj:
    def __init__(self, **kw): self.__dict__.update(kw)

class FakeK8s:
    # The "current" source has already rotated. If adopt() consults anything
    # live for the token, it can only get this one.
    current_pool_token = ROTATED
    def get_claim(self, n):
        calls.append(("get_claim", n))
        return Obj(ready=True, sandbox_name="sbx-1")
    def get_sandbox(self, n):
        calls.append(("get_sandbox", n))
        return Obj(ready=True, operating_mode="Running")

class FakeAffinity:
    def __init__(self, rec): self.rec = rec
    def get(self, k): calls.append(("affinity.get", k)); return self.rec
    def touch(self, k, ttl): calls.append(("affinity.touch", k, ttl)); return True

# The route was written an hour+ ago, then round-tripped through Valkey.
original = RouteRecord(handle=SandboxHandle(
    thread_key="T1", claim_name="c-1", sandbox_name="sbx-1", namespace="curie",
    service_fqdn="sbx-1.curie.svc", port=8080, session_id="s-1", token=PINNED))
raw = original.to_json()
assert PINNED in raw, "token is not persisted with the route at all"
rehydrated = RouteRecord.from_json(raw)

cfg = SubstrateConfig(namespace="curie", warm_pool="curie-runner-pool")
sub = SandboxSubstrate(FakeK8s(), FakeAffinity(rehydrated), cfg)
handle = sub.adopt("T1")

print(f"  route_ttl_seconds default        {cfg.route_ttl_seconds}")
print(f"  suspended_route_ttl_seconds      {cfg.suspended_route_ttl_seconds}")
print(f"  token persisted through Valkey   {'yes' if PINNED in raw else 'NO'}")
print(f"  source has rotated to            gen-two")
print(f"  adopt() presented                {'gen-one (pinned)' if handle.token == PINNED else 'gen-two (ROTATED!)' if handle.token == ROTATED else repr(handle.token)}")
print(f"  touch refreshed the TTL          {[c for c in calls if c[0]=='affinity.touch']}")
assert handle.token == PINNED, f"adopt() did NOT pin the token: {handle.token!r}"
assert ("affinity.touch", "T1", cfg.route_ttl_seconds) in calls, "no TTL refresh on adopt"
print("\n  PASS  the adopted thread keeps the token its pod booted with")
print(f"  PASS  adopt() touched the route, so {cfg.route_ttl_seconds}s is an idle bound, not a cap")
