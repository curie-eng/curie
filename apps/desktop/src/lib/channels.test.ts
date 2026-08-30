import { describe, expect, it } from "vitest";

import { channelLabel, channelsOf, primaryChannel } from "./channels";

describe("an agent's bindings", () => {
  const agent = (channels: unknown) => ({ channels }) as never;

  it("reads the shape the platform actually sends", () => {
    // The app read `channel.channel_id` for a long time, which this API has
    // never sent -- so every view said "no channel bound" about agents that
    // were answering in Slack.
    const a = agent([{ kind: "slack", address: "C0LOCALDEV" }]);
    expect(primaryChannel(a)).toEqual({ kind: "slack", address: "C0LOCALDEV" });
    expect(channelLabel(a)).toBe("slack · C0LOCALDEV");
  });

  it("counts the rest rather than listing them", () => {
    const a = agent([
      { kind: "slack", address: "C0AAA" },
      { kind: "slack", address: "C0BBB" },
      { kind: "slack", address: "C0CCC" },
    ]);
    expect(channelLabel(a)).toBe("slack · C0AAA +2");
    expect(channelsOf(a)).toHaveLength(3);
  });

  it("says nothing when there is genuinely nothing bound", () => {
    expect(channelLabel(agent([]))).toBeUndefined();
    expect(channelLabel(agent(null))).toBeUndefined();
    expect(channelLabel(agent(undefined))).toBeUndefined();
    expect(primaryChannel(agent([]))).toBeUndefined();
  });

  it("ignores a binding with no address rather than rendering a blank one", () => {
    expect(channelsOf(agent([{ kind: "slack" }, { kind: "slack", address: "C0X" }]))).toEqual([
      { kind: "slack", address: "C0X" },
    ]);
  });
});
