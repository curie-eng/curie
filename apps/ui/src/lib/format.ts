// The OB1 metrics API sources latency from Langfuse's metrics `latency` measure,
// which is reported in MILLISECONDS, now reflected in the `latency_p95_ms` field name
// (the Langfuse trace object's own `latency` is seconds, but the aggregate
// metrics measure is ms). Formatting that value directly as seconds overstated
// latency by 1000x. Convert here, in one place, so every display reads honest
// units: sub-second values as ms, larger values as seconds.
export function formatLatency(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return "—";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  return `${(ms / 1000).toFixed(2)}s`;
}

// A minimal shape covering both the read (`ChannelBinding`) and write
// (`ChannelBindingWrite`) channel types, so these helpers work on either
// without importing api/client and creating a cycle.
interface ChannelIdentity {
  kind: string;
  address: string;
  adapter?: string | null;
}

// The identity worth showing for one channel binding, or null (ADR-0168
// decision 3). "default" is the pre-ADR Slack app every install already had,
// so a Slack binding naming it shows none. Only Slack has a default identity:
// another kind's adapter is its credential slug and reads as it is, whatever
// it is called. A missing `adapter` (an API that predates the ADR, or a
// non-Slack binding with no route stored) shows none either.
export function channelNamedIdentity(binding: ChannelIdentity): string | null {
  if (!binding.adapter) return null;
  return binding.kind === "slack" && binding.adapter === "default" ? null : binding.adapter;
}

// Display text for one channel binding: the address, and beside it the
// identity `channelNamedIdentity` shows.
export function channelIdentityLabel(binding: ChannelIdentity): string {
  const identity = channelNamedIdentity(binding);
  return identity ? `${binding.address} (${identity})` : binding.address;
}

// React list key for one channel binding: the full route, not only the pair.
// Migration 0023's constraint still holds one row per `(kind, address)`, but a
// read-only list keyed on the pair would collide the moment two identities
// share one, so it keys on what ADR-0168 decision 3 makes the route.
export function channelIdentityKey(binding: ChannelIdentity): string {
  return `${binding.kind}:${binding.adapter ?? ""}:${binding.address}`;
}
