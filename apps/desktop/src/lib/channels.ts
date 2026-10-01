// Where an agent can be reached.
//
// The platform moved to one agent holding SEVERAL bindings (ADR-0118) and the
// API says so: `channels: [{ kind, address }]`. This app was still reading
// `channel: { kind, channel_id }`, a shape the API has not sent for some time,
// so every read of it was `undefined` and every view that asked said the same
// wrong thing -- "no channel bound" under an agent answering in Slack all day.
// Nothing failed; it just quietly reported the opposite of the truth.

export interface AgentChannel {
  readonly kind?: string;
  readonly address?: string;
}

interface HasChannels {
  readonly channels?: readonly AgentChannel[] | null;
}

/** Every binding, in the order the platform returns them. */
export function channelsOf(agent: HasChannels): readonly AgentChannel[] {
  return (agent.channels ?? []).filter((c) => !!c?.address);
}

/** The one to show when there is room for one. First is the platform's own
 *  order, which is the order they were added. */
export function primaryChannel(agent: HasChannels): AgentChannel | undefined {
  return channelsOf(agent)[0];
}

/** `slack · C0LOCALDEV`, or `slack · C0LOCALDEV +1` when there are more. A count
 *  rather than a list: the row has one line, and "there are others" is the part
 *  that changes what you do next. */
export function channelLabel(agent: HasChannels): string | undefined {
  const all = channelsOf(agent);
  if (!all.length) return undefined;
  const [first, ...rest] = all;
  const base = `${first.kind ?? "channel"} · ${first.address}`;
  return rest.length ? `${base} +${rest.length}` : base;
}
