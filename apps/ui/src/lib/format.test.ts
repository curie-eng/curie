import { describe, expect, it } from "vitest";
import { channelIdentityKey, channelIdentityLabel, channelNamedIdentity, formatLatency } from "./format";

describe("formatLatency", () => {
  it("renders sub-second latency in milliseconds", () => {
    expect(formatLatency(17.9)).toBe("18ms");
    expect(formatLatency(999)).toBe("999ms");
    expect(formatLatency(0)).toBe("0ms");
  });

  it("renders one-second-and-up latency in seconds with two decimals", () => {
    expect(formatLatency(1000)).toBe("1.00s");
    expect(formatLatency(2100)).toBe("2.10s");
    // The live repro: a ~6.3s p95 came back as 6292 (ms) and was printed as
    // "6292.00s". It must read as seconds, not the raw millisecond count.
    expect(formatLatency(6292)).toBe("6.29s");
  });

  it("returns a neutral dash for non-finite or negative input", () => {
    expect(formatLatency(NaN)).toBe("—");
    expect(formatLatency(-5)).toBe("—");
  });
});

describe("channelIdentityLabel", () => {
  it("shows only the address for a default, missing or null identity", () => {
    expect(channelIdentityLabel({ kind: "slack", address: "C0EXAMPLE1", adapter: "default" })).toBe(
      "C0EXAMPLE1",
    );
    expect(channelIdentityLabel({ kind: "slack", address: "C0EXAMPLE1", adapter: null })).toBe(
      "C0EXAMPLE1",
    );
    expect(channelIdentityLabel({ kind: "slack", address: "C0EXAMPLE1" })).toBe("C0EXAMPLE1");
  });

  it("appends a non-default identity beside the address", () => {
    expect(channelIdentityLabel({ kind: "slack", address: "C0EXAMPLE1", adapter: "finance" })).toBe(
      "C0EXAMPLE1 (finance)",
    );
  });
});

describe("channelNamedIdentity", () => {
  it("suppresses \"default\" only on Slack, the one kind with a default identity", () => {
    expect(channelNamedIdentity({ kind: "slack", address: "C0EXAMPLE1", adapter: "default" })).toBeNull();
    expect(channelNamedIdentity({ kind: "email", address: "ops@example.com", adapter: "default" })).toBe(
      "default",
    );
  });
});

describe("channelIdentityKey", () => {
  it("keys two bindings on the same pair differently when their identity differs", () => {
    const a = channelIdentityKey({ kind: "slack", address: "C0EXAMPLE1", adapter: "finance" });
    const b = channelIdentityKey({ kind: "slack", address: "C0EXAMPLE1", adapter: "default" });
    expect(a).not.toBe(b);
  });

  it("treats a missing adapter the same as an explicit null", () => {
    expect(channelIdentityKey({ kind: "slack", address: "C0EXAMPLE1" })).toBe(
      channelIdentityKey({ kind: "slack", address: "C0EXAMPLE1", adapter: null }),
    );
  });
});
