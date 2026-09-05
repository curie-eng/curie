// These tests exist because a duplicate row and a section that lost its header
// shipped past a typecheck, a lint and 84 other tests, and were only caught by
// looking at a screenshot. Grouping is table-shaped logic; it belongs in a
// function that can be asserted, not inside a component.

import { describe, expect, it } from "vitest";

import type { ResourceSample } from "../bridge/bridge";
import { NO_AGENT_OTHER, NO_AGENT_RUNNER, NO_PROJECT, aggregate, capacityNotes, groupRows, matches, selectRows } from "./workloads";
import type { DaemonCapacity } from "../../electron/shared/contract";

function sample(over: Partial<ResourceSample> & { name: string }): ResourceSample {
  return {
    id: over.name.slice(0, 12),
    origin: "docker",
    project: null,
    service: null,
    role: "other",
    state: "running",
    health: null,
    exitCode: null,
    cpuPercent: 1,
    memBytes: 1024,
    memLimitBytes: null,
    netRxBytes: 0,
    netTxBytes: 0,
    blockReadBytes: null,
    blockWriteBytes: null,
    pids: 1,
    startedAt: new Date("2026-08-24T12:00:00Z").toISOString(),
    image: "img:1",
    ports: [],
    at: 0,
    ...over,
  };
}

const STACK: ResourceSample[] = [
  sample({ name: "curie-curie-api-1", project: "curie", service: "curie-api", role: "api", cpuPercent: 4, ports: [{ host: 28000, container: 8000, proto: "tcp" }] }),
  sample({ name: "curie-curie-worker-1", project: "curie", service: "curie-worker", role: "worker", cpuPercent: 7 }),
  sample({ name: "curie-postgres-1", project: "curie", service: "postgres", role: "postgres", cpuPercent: 2, ports: [{ host: 25432, container: 5432, proto: "tcp" }] }),
  sample({ name: "curie-rustfs-init-1", project: "curie", service: "rustfs-init", role: "objectstore", state: "exited", cpuPercent: null, memBytes: null }),
  sample({ name: "curie-runner-deal-desk", role: "runner", agent: "deal-desk", cpuPercent: 45 }),
  sample({ name: "curie-runner-local", role: "runner", cpuPercent: 3 }),
];

describe("grouping partitions the rows", () => {
  it("puts every row in exactly one section, once", () => {
    // The bug this replaces rendered one container twice and dropped another
    // section's header.
    for (const group of ["project", "agent", "role"] as const) {
      const sections = groupRows(STACK, group);
      const names = sections.flatMap((s) => s.rows.map((r) => r.name));
      expect(names.length, group).toBe(STACK.length);
      expect(new Set(names).size, group).toBe(STACK.length);
    }
  });

  it("gives every section a unique key, so rows cannot be mis-keyed", () => {
    for (const group of ["project", "agent", "role"] as const) {
      const keys = groupRows(STACK, group).map((s) => s.key);
      expect(new Set(keys).size, group).toBe(keys.length);
    }
  });

  it("groups compose members under their project and runners as standalone", () => {
    const sections = groupRows(STACK, "project");
    const curie = sections.find((s) => s.key === "curie");
    const standalone = sections.find((s) => s.key === NO_PROJECT);
    expect(curie?.rows).toHaveLength(4);
    expect(standalone?.rows).toHaveLength(2);
  });

  it("names the bucket rather than leaving it empty", () => {
    const byAgent = groupRows(STACK, "agent").map((s) => s.label);
    expect(byAgent).toContain("deal-desk");
    expect(byAgent).toContain(NO_AGENT_RUNNER);
    expect(byAgent).toContain(NO_AGENT_OTHER);
    expect(byAgent.every((l) => l.trim().length > 0)).toBe(true);
  });

  it("sorts infrastructure buckets last", () => {
    const labels = groupRows(STACK, "agent").map((s) => s.label);
    expect(labels[labels.length - 1]).toBe(NO_AGENT_OTHER);
  });

  it("returns no sections at all for an empty list", () => {
    expect(groupRows([], "project")).toEqual([]);
    expect(groupRows([], "none")).toEqual([]);
  });

  it("flat mode is one section holding everything", () => {
    const sections = groupRows(STACK, "none");
    expect(sections).toHaveLength(1);
    expect(sections[0].rows).toHaveLength(STACK.length);
  });
});

describe("search", () => {
  it("finds a container by its published host port", () => {
    expect(selectRows(STACK, { query: "28000", sort: "cpu" }).map((r) => r.name)).toEqual([
      "curie-curie-api-1",
    ]);
  });

  it("finds a container by its container-side port, which is what the compose file says", () => {
    expect(selectRows(STACK, { query: "5432", sort: "cpu" }).map((r) => r.name)).toEqual([
      "curie-postgres-1",
    ]);
  });

  it("does not match a port that merely contains the digits of another", () => {
    // 28123 must not answer a search for 28000.
    const clickhouse = sample({
      name: "curie-clickhouse-1",
      project: "curie",
      ports: [{ host: 28123, container: 8123, proto: "tcp" }],
    });
    expect(matches(clickhouse, "28000")).toBe(false);
  });

  it("matches project, service, role and agent", () => {
    expect(selectRows(STACK, { query: "curie", sort: "name" }).length).toBeGreaterThan(1);
    expect(selectRows(STACK, { query: "deal-desk", sort: "name" }).map((r) => r.name)).toEqual([
      "curie-runner-deal-desk",
    ]);
    expect(selectRows(STACK, { query: "postgres", sort: "name" })).toHaveLength(1);
  });

  it("an empty query keeps everything", () => {
    expect(selectRows(STACK, { query: "   ", sort: "cpu" })).toHaveLength(STACK.length);
  });

  it("combines with the running-only filter", () => {
    const rows = selectRows(STACK, { runningOnly: true, sort: "cpu" });
    expect(rows).toHaveLength(STACK.length - 1);
    expect(rows.every((r) => r.state === "running")).toBe(true);
  });
});

describe("sorting", () => {
  it("puts the busiest first and unmeasurable last", () => {
    const rows = selectRows(STACK, { sort: "cpu" });
    expect(rows[0].name).toBe("curie-runner-deal-desk");
    expect(rows[rows.length - 1].name).toBe("curie-rustfs-init-1");
  });

  it("is stable for equal values, so the table does not shuffle every frame", () => {
    const tied = [
      sample({ name: "b-one", cpuPercent: 5 }),
      sample({ name: "a-two", cpuPercent: 5 }),
      sample({ name: "c-three", cpuPercent: 5 }),
    ];
    expect(selectRows(tied, { sort: "cpu" }).map((r) => r.name)).toEqual([
      "a-two",
      "b-one",
      "c-three",
    ]);
  });
});

describe("aggregate", () => {
  it("reports mixed state when a project has both running and stopped members", () => {
    const curie = groupRows(STACK, "project").find((s) => s.key === "curie")!;
    const agg = aggregate(curie.rows);
    expect(agg.state).toBe("mixed");
    expect(agg.running).toBe(3);
    expect(agg.total).toBe(4);
  });

  it("sums only what was measurable", () => {
    const agg = aggregate(STACK.filter((r) => r.project === "curie"));
    // 4 + 7 + 2, with the exited member contributing nothing rather than zero.
    expect(agg.cpu).toBe(13);
  });

  it("stays null when nothing was measurable, rather than claiming zero", () => {
    const stopped = [
      sample({ name: "x", state: "exited", cpuPercent: null, memBytes: null }),
      sample({ name: "y", state: "exited", cpuPercent: null, memBytes: null }),
    ];
    const agg = aggregate(stopped);
    expect(agg.cpu).toBeNull();
    expect(agg.mem).toBeNull();
    expect(agg.state).toBe("stopped");
  });

  it("reports running when every member is up", () => {
    expect(aggregate(STACK.filter((r) => r.role === "runner")).state).toBe("running");
  });

  it("takes the most recent start time", () => {
    const rows = [
      sample({ name: "old", startedAt: new Date("2026-08-01T00:00:00Z").toISOString() }),
      sample({ name: "new", startedAt: new Date("2026-08-24T00:00:00Z").toISOString() }),
    ];
    expect(aggregate(rows).startedAt).toBe(Date.parse("2026-08-24T00:00:00Z"));
  });
});

describe("capacityNotes", () => {
  // The case that prompted this: a 36 GB machine showing a 7.7 GB ceiling. The
  // number was right -- containers cannot exceed the Docker VM -- but unlabelled
  // it reads as a bug rather than as a limit you can raise.
  const cap = (over: Partial<DaemonCapacity> = {}): DaemonCapacity => ({
    cpus: 12,
    memBytes: 8_217_165_824,
    serverVersion: "29.1.3",
    hostCpus: 12,
    hostMemBytes: 38_654_705_664,
    ...over,
  });

  it("names the daemon's memory as a limit, with the machine's figure", () => {
    expect(capacityNotes(cap()).mem).toBe("Docker's limit, of 36.0 GB on this machine");
  });

  it("does not call it a limit when the daemon has essentially all of it", () => {
    // A Linux host, where the daemon sees the machine. 35.9 of 36 is rounding.
    const note = capacityNotes(cap({ memBytes: 38_000_000_000 })).mem;
    expect(note).toBe("Docker 29.1.3");
  });

  it("says how many CPUs Docker has when it is fewer than the machine's", () => {
    expect(capacityNotes(cap({ cpus: 4 })).cpu).toBe("4 of 12 CPUs, Docker's share");
  });

  it("does not imply a CPU limit when there is none", () => {
    expect(capacityNotes(cap()).cpu).toBe("12 CPUs available");
  });

  it("gets the singular right", () => {
    expect(capacityNotes(cap({ cpus: 1, hostCpus: 1 })).cpu).toBe("1 CPU available");
  });

  it("admits it does not know rather than inventing a ceiling", () => {
    expect(capacityNotes(null)).toEqual({ cpu: "capacity unknown", mem: "capacity unknown" });
    expect(capacityNotes(cap({ cpus: null })).cpu).toBe("capacity unknown");
    expect(capacityNotes(cap({ memBytes: null, serverVersion: null })).mem).toBe("capacity unknown");
  });

  it("falls back to the version when the host total is unknown", () => {
    // No host figure means no gap can be claimed either way.
    expect(capacityNotes(cap({ hostMemBytes: null })).mem).toBe("Docker 29.1.3");
  });
});
