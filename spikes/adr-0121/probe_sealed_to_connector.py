"""Seal to the CONNECTOR, not to the cluster.

The four ways out of the ADR-0121/0122 tension all argue about which platform
component holds the key. That is the wrong question. Exactly one party ever needs
to read a snapshot: the connector that produced it. Not the API, not the worker,
not the runner.

So the connector seals its own snapshot to its own key. The platform stores a
blob it CANNOT read, and every executor -- runner, worker, anything -- carries it
without being able to open it. The executor's location stops mattering for
confidentiality, which is the tension dissolved rather than traded away.

This probes the mechanism and the one thing it breaks: the conflict check.
"""

import base64
import hashlib
import json
import sys

from nacl.public import PrivateKey, SealedBox

SNAPSHOT = {
    "spec": {"template": {"spec": {"containers": [{"env": [
        {"name": "API_TOKEN", "value": "Bearer sk-live-abcdef123456"},
    ]}]}}}
}


def main() -> int:
    # The connector's own keypair, mounted like its kubeconfig is.
    connector_key = PrivateKey.generate()

    print("== the connector seals its own snapshot")
    blob = base64.b64encode(
        SealedBox(connector_key.public_key).encrypt(json.dumps(SNAPSHOT).encode())
    ).decode()
    print(f"   ledger stores {len(blob)} opaque chars")
    print(f"   platform can read the token: {'sk-live' in blob}")

    print("\n== every executor carries it without opening it")
    for who in ("runner (sandbox)", "worker", "api"):
        try:
            SealedBox(PrivateKey.generate()).decrypt(base64.b64decode(blob))
            print(f"   {who:18} opened it  <-- would be a finding")
        except Exception:
            print(f"   {who:18} cannot open it")

    print("\n== the connector opens its own")
    opened = json.loads(SealedBox(connector_key).decrypt(base64.b64decode(blob)))
    print(f"   round-trips byte-exact: {opened == SNAPSHOT}")

    print("\n== what this breaks, and the fix")
    print("   ADR-0117 decision 4 compares the live state to `post_state`. A sealed")
    print("   box is non-deterministic, so two seals of the same state differ:")
    a = SealedBox(connector_key.public_key).encrypt(b"same")
    b = SealedBox(connector_key.public_key).encrypt(b"same")
    print(f"     seal(x) == seal(x): {a == b}   <- cannot compare ciphertexts")
    print("   So the comparison moves off the state and onto a VERSION TOKEN the")
    print("   connector reports -- resourceVersion, ETag, generation. Opaque to the")
    print("   platform, comparable by equality, and a better question than state")
    print("   equality anyway: it catches a change that reverted to the same value,")
    print("   and it does not false-positive on managedFields churn.")
    left, observed = "kube-rv-8891", "kube-rv-9004"
    print(f"     left={left!r} observed={observed!r} -> "
          f"{'refuse' if left != observed else 'permit'}")
    print(f"   digest of a state is the fallback where no version exists: "
          f"{hashlib.sha256(json.dumps(SNAPSHOT, sort_keys=True).encode()).hexdigest()[:16]}...")
    return 0


if __name__ == "__main__":
    sys.exit(main())
