// The stack-coming-up counter.
//
// Every case here is one the screen gets wrong in a specific, visible way if the
// rule is off: a stack stuck at "8 of 10" that is actually up, a red error
// flashing between the last healthcheck and the next API poll, or a broken
// service rendered as a slow one.

import { describe, expect, it } from "vitest";

import type { ResourceSample } from "../bridge/bridge";
import { SETTLE_GRACE_MS, stackPhase, stackProgress } from "./startup";

function c(over: Partial<ResourceSample> & { service: string | null }): ResourceSample {
  return {
    id: over.service ?? "x",
    name: over.service ?? "x",
    origin: "docker",
    project: over.service ? "curie" : null,
    role: over.service ?? "runner",
    state: "running",
    health: null,
    exitCode: null,
    cpuPercent: null,
    memBytes: null,
    memLimitBytes: null,
    netRxBytes: null,
    netTxBytes: null,
    blockReadBytes: null,
    blockWriteBytes: null,
    pids: null,
    startedAt: null,
    image: null,
    ports: [],
    at: 0,
    ...over,
  };
}

describe("stackProgress", () => {
  it("counts a running container with no healthcheck as ready", () => {
    // The case that would otherwise never finish: most compose services declare
    // no healthcheck, so a missing verdict has to mean "no opinion", not
    // "starting".
    const p = stackProgress([c({ service: "otel-collector" })]);
    expect(p).toMatchObject({ total: 1, ready: 1, waiting: [], failed: [] });
  });

  it("counts a healthy container as ready and a starting one as waiting", () => {
    const p = stackProgress([
      c({ service: "postgres", health: "healthy" }),
      c({ service: "curie-api", health: "starting" }),
    ]);
    expect(p.ready).toBe(1);
    expect(p.waiting).toEqual(["curie-api"]);
  });

  it("counts a one-shot that exited 0 as ready, not as broken", () => {
    // `curie-migrate`, `rustfs-init` and the two `*-perms` containers run once
    // and exit. Reading "stopped" as "broken" reported four failures on a stack
    // that was completely healthy.
    const p = stackProgress([
      c({ service: "curie-migrate", state: "exited", exitCode: 0 }),
      c({ service: "rustfs-init", state: "exited", exitCode: 0 }),
    ]);
    expect(p).toMatchObject({ total: 2, ready: 2, waiting: [], failed: [] });
  });

  it("still calls a non-zero exit a failure", () => {
    const p = stackProgress([c({ service: "curie-migrate", state: "exited", exitCode: 1 })]);
    expect(p.failed).toEqual(["curie-migrate"]);
  });

  it("separates a broken service from a slow one", () => {
    const p = stackProgress([
      c({ service: "curie-api", health: "unhealthy" }),
      c({ service: "worker", state: "exited", exitCode: 137 }),
      c({ service: "valkey", health: "starting" }),
    ]);
    expect(p.failed).toEqual(["curie-api", "worker"]);
    expect(p.waiting).toEqual(["valkey"]);
    expect(p.ready).toBe(0);
  });

  it("treats `created` as still coming, not as failed", () => {
    // Compose creates every container before it starts waiting on any of them.
    const p = stackProgress([c({ service: "clickhouse", state: "created" })]);
    expect(p.waiting).toEqual(["clickhouse"]);
    expect(p.failed).toEqual([]);
  });

  it("ignores containers that are not part of a compose project", () => {
    // A `curie skill up` runner is not the local stack.
    const p = stackProgress([c({ service: null, role: "runner" })]);
    expect(p.total).toBe(0);
  });
});

describe("stackPhase", () => {
  const none = { total: 0, ready: 0, waiting: [], failed: [] };

  it("reports `up` rather than disappearing once everything is answering", () => {
    // The card used to vanish here, which meant the only thing telling an
    // operator it had worked was the absence of a warning -- success by
    // removal, which only works if you were watching.
    const up = { total: 4, ready: 4, waiting: [], failed: [] };
    expect(stackPhase(up, { apiReachable: true, runActive: false })).toBe("up");
  });

  it("is `absent` when there is no stack at all -- the first-run case", () => {
    // Nothing created is not a stack that is down, it is one nobody has asked
    // for yet, and that is the single moment the app has one obvious next move.
    expect(stackPhase(none, { apiReachable: false, runActive: false })).toBe("absent");
    expect(stackPhase(none, { apiReachable: true, runActive: false })).toBe("absent");
  });

  it("keeps showing the card during a run even when the API already answers", () => {
    // `local rebuild`, or `up` run a second time, both act on a stack that is
    // already answering. Keying the card off the API meant it vanished for
    // exactly those -- and made the card's existence depend on the same fact
    // the errors beside it are about.
    const up = { total: 4, ready: 4, waiting: [], failed: [] };
    expect(stackPhase(up, { apiReachable: true, runActive: true })).toBe("starting");
  });

  it("is starting with nothing created yet, but only while a run is in flight", () => {
    // Pulling images is the longest phase and emits no output; a blank screen
    // through it is what makes the whole thing feel broken. With no run, the
    // same nothing is a first run rather than a start in progress.
    expect(stackPhase(none, { apiReachable: false, runActive: true })).toBe("starting");
    expect(stackPhase(none, { apiReachable: false, runActive: false })).toBe("absent");
  });

  it("is starting when containers exist and some are not ready, with no run of ours", () => {
    // Someone may have run `curie local up` in a terminal. Docker still knows.
    const p = { total: 4, ready: 2, waiting: ["curie-api", "worker"], failed: [] };
    expect(stackPhase(p, { apiReachable: false, runActive: false })).toBe("starting");
  });

  it("settles rather than erroring between the last healthcheck and the API poll", () => {
    const p = { total: 4, ready: 4, waiting: [], failed: [] };
    expect(stackPhase(p, { apiReachable: false, runActive: false })).toBe("settling");
  });

  it("stops settling and lets the error through once the grace period is up", () => {
    // Every container healthy and still no API means something is actually
    // wrong. A spinner that never resolves would hide that forever behind a
    // message saying it is fine.
    const p = { total: 4, ready: 4, waiting: [], failed: [] };
    expect(
      stackPhase(p, { apiReachable: false, runActive: false, settlingForMs: SETTLE_GRACE_MS + 1 }),
    ).toBe("idle");
    expect(
      stackPhase(p, { apiReachable: false, runActive: false, settlingForMs: SETTLE_GRACE_MS - 1 }),
    ).toBe("settling");
  });
});
