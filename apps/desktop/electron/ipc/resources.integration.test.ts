// Integration: run the resource feed against the real Docker daemon.
//
// The unit tests check the parsers against strings I wrote down. This checks
// them against strings Docker actually emits today, on this machine, with
// whatever containers happen to be up -- which is the only way to catch a format
// that shifted under us. It asserts shape and invariants rather than specific
// containers, so it does not care what is running.
//
// Skipped when Docker is not reachable.

import { beforeAll, describe, expect, it } from "vitest";

import { collect, daemonCapacity, dockerAvailable, resetCapacityCache } from "./resources";

let available = false;
beforeAll(async () => {
  available = await dockerAvailable();
}, 30_000);

const withDocker = describe;

withDocker("against the live daemon", () => {
  it("reports a capacity with a real CPU count and memory total", async () => {
    if (!available) return;
    resetCapacityCache();
    const capacity = await daemonCapacity();
    expect(capacity).not.toBeNull();
    // These are the denominators every percentage in the UI is read against, so
    // a zero or a null here would silently make the headline numbers meaningless.
    expect(capacity!.cpus).toBeGreaterThan(0);
    expect(capacity!.memBytes).toBeGreaterThan(256 * 1024 * 1024);
    expect(capacity!.serverVersion).toMatch(/\d+\./);
  }, 30_000);

  it("caches the capacity rather than asking the daemon every frame", async () => {
    if (!available) return;
    resetCapacityCache();
    const first = await daemonCapacity();
    const started = Date.now();
    const second = await daemonCapacity();
    // The second call must come from cache; `docker info` is a round trip and
    // this runs on every sample tick.
    expect(Date.now() - started).toBeLessThan(50);
    expect(second).toEqual(first);
  }, 30_000);

  it("produces a frame whose samples all satisfy the contract", async () => {
    if (!available) return;
    const frame = await collect();
    expect(frame.error).toBeUndefined();
    expect(frame.capacity).not.toBeNull();

    for (const s of frame.samples) {
      // Identity
      expect(s.name.length).toBeGreaterThan(0);
      expect(s.id.length).toBeGreaterThan(0);
      expect(s.origin).toBe("docker");
      expect(typeof s.role).toBe("string");

      // Unmeasurable must be null, never a NaN that formats as garbage.
      for (const n of [
        s.cpuPercent,
        s.memBytes,
        s.memLimitBytes,
        s.netRxBytes,
        s.netTxBytes,
        s.pids,
      ]) {
        expect(n === null || Number.isFinite(n), `${s.name}: ${n}`).toBe(true);
      }

      // A stopped container has no stats row; it must not claim 0%.
      if (s.state !== "running") expect(s.cpuPercent).toBeNull();

      // Ports
      for (const p of s.ports) {
        expect(Number.isFinite(p.container)).toBe(true);
        expect(p.host === null || Number.isFinite(p.host)).toBe(true);
        expect(p.proto).toMatch(/^(tcp|udp|sctp)$/);
      }
      // The v4/v6 collapse means no two bindings can be identical.
      const keys = s.ports.map((p) => `${p.host}:${p.container}/${p.proto}`);
      expect(new Set(keys).size).toBe(keys.length);

      // A compose member has both a project and a service, or neither.
      if (s.service !== null) expect(s.project).not.toBeNull();
    }
  }, 40_000);

  it("gives every container in a frame a unique name, which the table keys on", async () => {
    if (!available) return;
    const frame = await collect();
    const names = frame.samples.map((s) => s.name);
    // The resource table keys rows by name. If Docker could ever return two
    // containers with the same one, React would render a duplicate row -- which
    // is exactly the bug a truncated-id key produced.
    expect(new Set(names).size).toBe(names.length);
  }, 40_000);

  it("groups compose members of one project under one project name", async () => {
    if (!available) return;
    const frame = await collect();
    const members = frame.samples.filter((s) => s.service !== null);
    for (const m of members) {
      expect(typeof m.project).toBe("string");
      expect(m.project!.length).toBeGreaterThan(0);
    }
  }, 40_000);
});
