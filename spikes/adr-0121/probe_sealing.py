"""Can the PLATFORM seal a snapshot, and who can open it? (ADR-0122)

ADR-0122 proposes sealing `prior_state` "through the credential sealing that
already exists". That seam runs the other way -- the Rust CLI seals, the Python
worker opens -- so this probes whether the direction is reusable at all, and
what the answer costs.
"""

import base64
import json
import sys

from nacl.public import PrivateKey, SealedBox

SNAPSHOT = {
    "spec": {"template": {"spec": {"containers": [{"env": [
        {"name": "API_TOKEN", "value": "Bearer sk-live-abcdef123456"},
        {"name": "REPLICAS", "value": "3"},
    ]}]}}}
}


def main() -> int:
    # The cluster keypair, as the chart mounts it into the worker.
    cluster_private = PrivateKey.generate()
    cluster_public = cluster_private.public_key

    print("== sealing needs only the PUBLIC half")
    blob = base64.b64encode(SealedBox(cluster_public).encrypt(json.dumps(SNAPSHOT).encode())).decode()
    print(f"   sealed {len(blob)} b64 chars; token visible in blob: "
          f"{'sk-live' in blob}")

    print("\n== opening needs the PRIVATE half")
    opened = json.loads(SealedBox(cluster_private).decrypt(base64.b64decode(blob)))
    env = opened["spec"]["template"]["spec"]["containers"][0]["env"]
    print(f"   round-trips byte-exact: {opened == SNAPSHOT}")
    print(f"   token survives intact:  {env[0]['value']!r}")

    print("\n== who holds which half, today")
    print("   worker : CURIE_SEALING_PRIVATE_KEY  -> can open, and can derive the")
    print("            public half, so it can also SEAL to itself")
    print("   runner : holds neither -- by design, it is the sandbox")
    print("   api    : holds neither today")

    print("\n== the tension this exposes")
    print("   ADR-0121 puts the executor in the RUNNER, in a sandbox.")
    print("   ADR-0122 seals prior_state to the CLUSTER key.")
    print("   A sealed snapshot the runner cannot open is a restore the runner")
    print("   cannot perform. The two drafts do not compose as written.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
