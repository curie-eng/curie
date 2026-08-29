// Docker's stats output is a human-readable format that this app parses back
// into numbers, which is exactly the kind of code that quietly goes wrong. The
// unit mixing is real: Docker prints memory in binary units (MiB, GiB) and
// network in SI ones (kB, MB) in the same row.

import { describe, expect, it } from "vitest";

import { classify, parseBytes, parsePercent, parsePorts } from "./resources";

describe("parseBytes", () => {
  it("reads binary units as powers of 1024", () => {
    expect(parseBytes("1KiB")).toBe(1024);
    expect(parseBytes("1MiB")).toBe(1024 ** 2);
    expect(parseBytes("1.5GiB")).toBeCloseTo(1.5 * 1024 ** 3);
  });

  it("reads SI units as powers of 1000, which is what Docker prints for I/O", () => {
    expect(parseBytes("1kB")).toBe(1000);
    expect(parseBytes("2.5MB")).toBe(2_500_000);
  });

  it("reads bare bytes", () => {
    expect(parseBytes("0B")).toBe(0);
    expect(parseBytes("512B")).toBe(512);
    expect(parseBytes("  48B ")).toBe(48);
  });

  it("returns null rather than zero for anything it cannot read", () => {
    // The distinction matters all the way up to the UI: null renders as a dash,
    // zero would claim the container measured zero.
    expect(parseBytes(undefined)).toBeNull();
    expect(parseBytes("")).toBeNull();
    expect(parseBytes("--")).toBeNull();
    expect(parseBytes("N/A")).toBeNull();
  });
});

describe("parsePercent", () => {
  it("reads Docker's CPU column", () => {
    expect(parsePercent("0.00%")).toBe(0);
    expect(parsePercent("14.63%")).toBeCloseTo(14.63);
  });

  it("allows over 100, which is normal for a multi-core container", () => {
    expect(parsePercent("312.5%")).toBeCloseTo(312.5);
  });

  it("returns null for a missing or unreadable value", () => {
    expect(parsePercent(undefined)).toBeNull();
    expect(parsePercent("--")).toBeNull();
  });
});

describe("classify", () => {
  it("recognises a runner and attributes it to its agent", () => {
    expect(classify("curie-runner-deal-desk", {})).toEqual({ role: "runner", agent: "deal-desk" });
  });

  it("treats the default runner name as unattributed rather than an agent called local", () => {
    expect(classify("curie-runner-local", {})).toEqual({ role: "runner", agent: undefined });
  });

  it("reads the compose service label, and canonicalises it", () => {
    expect(classify("curie-postgres-1", { "com.docker.compose.service": "postgres" })).toEqual({
      role: "postgres",
      service: "postgres",
    });
    expect(classify("some-odd-name", { "com.docker.compose.service": "langfuse-web" })).toEqual({
      role: "langfuse",
      service: "langfuse-web",
    });
  });

  it("falls back to the name when there is no label", () => {
    expect(classify("curie-valkey-1", {}).role).toBe("valkey");
    expect(classify("curie-clickhouse-1", {}).role).toBe("clickhouse");
    expect(classify("curie-ollama", {}).role).toBe("model");
  });

  it("files an unrecognised container as other rather than guessing", () => {
    expect(classify("curie-something-new", {})).toEqual({ role: "other" });
  });
});

describe("parsePorts", () => {
  it("collapses the IPv4 and IPv6 rows for one published port", () => {
    // Docker lists each binding once per address family. Counting them
    // separately would tell an operator they published twice as many ports as
    // they did -- the exact number they are about to go looking for.
    expect(parsePorts("0.0.0.0:28000->8000/tcp, [::]:28000->8000/tcp")).toEqual([
      { host: 28000, container: 8000, proto: "tcp" },
    ]);
  });

  it("keeps genuinely distinct ports", () => {
    const parsed = parsePorts(
      "0.0.0.0:29000->9000/tcp, [::]:29000->9000/tcp, 0.0.0.0:29001->9001/tcp, [::]:29001->9001/tcp",
    );
    expect(parsed).toEqual([
      { host: 29000, container: 9000, proto: "tcp" },
      { host: 29001, container: 9001, proto: "tcp" },
    ]);
  });

  it("handles a loopback-only binding", () => {
    expect(parsePorts("127.0.0.1:5432->5432/tcp")).toEqual([
      { host: 5432, container: 5432, proto: "tcp" },
    ]);
  });

  it("marks an exposed-but-unpublished port with a null host", () => {
    // Exposed and published are different facts, and the UI renders them
    // differently: only a published port has somewhere to click.
    expect(parsePorts("5432/tcp")).toEqual([{ host: null, container: 5432, proto: "tcp" }]);
  });

  it("keeps a published port distinct from the same port merely exposed", () => {
    const parsed = parsePorts("6379/tcp, 0.0.0.0:26379->6379/tcp");
    expect(parsed).toHaveLength(2);
    expect(parsed.some((p) => p.host === 26379)).toBe(true);
    expect(parsed.some((p) => p.host === null)).toBe(true);
  });

  it("sorts by the port an operator would type", () => {
    const parsed = parsePorts("0.0.0.0:29001->9001/tcp, 0.0.0.0:25432->5432/tcp");
    expect(parsed.map((p) => p.host)).toEqual([25432, 29001]);
  });

  it("returns nothing for a container with no ports", () => {
    expect(parsePorts("")).toEqual([]);
    expect(parsePorts(undefined)).toEqual([]);
  });

  it("skips a fragment it cannot read rather than emitting a NaN port", () => {
    const parsed = parsePorts("nonsense, 0.0.0.0:28000->8000/tcp");
    expect(parsed).toEqual([{ host: 28000, container: 8000, proto: "tcp" }]);
    expect(parsed.every((p) => Number.isFinite(p.container))).toBe(true);
  });

  it("preserves the protocol, so udp is not shown as tcp", () => {
    expect(parsePorts("0.0.0.0:5353->53/udp")[0].proto).toBe("udp");
  });
});

describe("classify canonicalises the role", () => {
  // The exact service names a real `curie local up` produces. The role used to be
  // the raw service name, which meant `curie-api` never matched a consumer
  // looking for `api` -- the canvas dropped most of the platform and drew no
  // edges at all. These are the names that broke it.
  const REAL: [string, string, string][] = [
    ["curie-curie-api-1", "curie-api", "api"],
    ["curie-curie-worker-1", "curie-worker", "worker"],
    ["curie-curie-dispatcher-1", "curie-dispatcher", "dispatcher"],
    ["curie-postgres-1", "postgres", "postgres"],
    ["curie-postgres-wal2json-1", "postgres-wal2json", "postgres"],
    ["curie-valkey-1", "valkey", "valkey"],
    ["curie-rustfs-1", "rustfs", "objectstore"],
  ];

  it.each(REAL)("maps %s (service %s) to role %s", (name, service, role) => {
    const out = classify(name, { "com.docker.compose.service": service });
    expect(out.role).toBe(role);
    // The service name is kept too: it is what a human recognises from the
    // compose file, and it is what the table shows.
    expect(out.service).toBe(service);
  });

  it("never returns a project-prefixed role for a known service", () => {
    for (const [name, service] of REAL) {
      const { role } = classify(name, { "com.docker.compose.service": service });
      expect(role.startsWith("curie-"), `${service} -> ${role}`).toBe(false);
    }
  });

  it("files one-shot containers as jobs, not as the thing they set up", () => {
    // `rustfs-init` must not be classified as the object store; it exits, and
    // drawing it next to the real store is noise.
    for (const [name, service] of [
      ["curie-rustfs-init-1", "rustfs-init"],
      ["curie-rustfs-perms-1", "rustfs-perms"],
      ["curie-curie-migrate-1", "curie-migrate"],
    ] as const) {
      expect(classify(name, { "com.docker.compose.service": service }).role).toBe("job");
    }
  });

  it("keeps langfuse's own worker out of the platform worker role", () => {
    // Pattern order is load bearing: `langfuse` must win over `worker`.
    expect(
      classify("curie-langfuse-worker-1", { "com.docker.compose.service": "langfuse-worker" }).role,
    ).toBe("langfuse");
    expect(
      classify("curie-langfuse-web-1", { "com.docker.compose.service": "langfuse-web" }).role,
    ).toBe("langfuse");
  });

  it("maps the collector and the local model", () => {
    expect(classify("curie-otel-collector-1", { "com.docker.compose.service": "otel-collector" }).role).toBe("otel");
    expect(classify("curie-ollama-1", { "com.docker.compose.service": "ollama" }).role).toBe("model");
  });

  it("falls back to the service name rather than discarding it", () => {
    const out = classify("curie-something-new-1", { "com.docker.compose.service": "something-new" });
    expect(out.role).toBe("something-new");
    expect(out.service).toBe("something-new");
  });

  it("leaves a standalone runner without a service, and keeps the agent", () => {
    expect(classify("curie-runner-local", {}).service).toBeUndefined();
    expect(classify("curie-runner-deal-desk", {})).toEqual({
      role: "runner",
      agent: "deal-desk",
    });
  });
});

describe("classify attributes the platform's own sandboxes", () => {
  // A turn runs in `curie-thread-<hash>-<hash>`, whose name carries a thread
  // hash and nothing else. Before the label was read these fell through to role
  // "other" with no agent -- the one container on the machine that IS an agent
  // doing work was the one the Resources tab could not attribute.
  const LABELS = {
    "curietech.ai/agent": "shift-notes",
    "curietech.ai/managed-by": "curie-sandbox-substrate",
    "curietech.ai/thread-hash": "670393b761",
  };

  it("names the agent a sandbox is running for", () => {
    expect(classify("curie-thread-670393b761-c62e9b", LABELS)).toEqual({
      role: "runner",
      agent: "shift-notes",
    });
  });

  it("prefers the label over anything the name suggests", () => {
    expect(classify("curie-runner-something-else", LABELS)).toEqual({
      role: "runner",
      agent: "shift-notes",
    });
  });

  it("still calls an unlabelled sandbox a runner rather than 'other'", () => {
    expect(classify("curie-thread-abc-def", {})).toEqual({ role: "runner" });
    expect(classify("x", { "curietech.ai/managed-by": "curie-sandbox-substrate" })).toEqual({
      role: "runner",
    });
  });

  it("leaves compose services alone", () => {
    expect(classify("curie-valkey-1", { "com.docker.compose.service": "valkey" })).toEqual({
      role: "valkey",
      service: "valkey",
    });
  });
});
