// Formatting for the numbers this app shows a lot of. All of it obeys one rule:
// an unknown value renders as an em dash, never as zero. A resource monitor
// that draws 0% for "could not measure" is worse than one that admits it.

export const DASH = "—";

export function bytes(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return DASH;
  if (value === 0) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB", "PB"];
  const i = Math.min(units.length - 1, Math.floor(Math.log(Math.abs(value)) / Math.log(1024)));
  const scaled = value / Math.pow(1024, i);
  // Whole bytes never want a decimal point; larger units read better with one.
  return `${scaled.toFixed(i === 0 ? 0 : scaled >= 100 ? 0 : digits)} ${units[i]}`;
}

export function percent(value: number | null | undefined, digits = 1): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return DASH;
  return `${value.toFixed(value >= 100 ? 0 : digits)}%`;
}

export function usd(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return DASH;
  if (value === 0) return "$0.00";
  return value < 0.01 ? `$${value.toFixed(4)}` : `$${value.toFixed(2)}`;
}

export function count(value: number | null | undefined): string {
  if (value === null || value === undefined || !Number.isFinite(value)) return DASH;
  if (Math.abs(value) >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`;
  if (Math.abs(value) >= 1_000) return `${(value / 1_000).toFixed(1)}k`;
  return String(Math.round(value));
}

export function duration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined || !Number.isFinite(ms)) return DASH;
  if (ms < 1000) return `${Math.round(ms)}ms`;
  const s = ms / 1000;
  if (s < 60) return `${s.toFixed(s < 10 ? 1 : 0)}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${Math.round(s % 60)}s`;
  const h = Math.floor(m / 60);
  return `${h}h ${m % 60}m`;
}

/** "3m ago" for timestamps in tables. Anything older than a week gets a date,
 *  because "9d ago" stops being useful once you are comparing weeks. */
export function ago(at: number | string | null | undefined): string {
  if (at === null || at === undefined) return DASH;
  const ts = typeof at === "string" ? Date.parse(at) : at;
  if (!Number.isFinite(ts)) return DASH;
  const delta = Date.now() - ts;
  if (delta < 0) return "just now";
  const s = delta / 1000;
  if (s < 45) return "just now";
  if (s < 90) return "1m ago";
  const m = s / 60;
  if (m < 60) return `${Math.round(m)}m ago`;
  const h = m / 60;
  if (h < 24) return `${Math.round(h)}h ago`;
  const d = h / 24;
  if (d < 7) return `${Math.round(d)}d ago`;
  return new Date(ts).toLocaleDateString();
}

export function clock(at: number | null | undefined): string {
  if (at === null || at === undefined) return DASH;
  return new Date(at).toLocaleTimeString([], { hour12: false });
}

// The CLI is told NO_COLOR, but a tool it shells out to (helm, docker, kubectl)
// may not honour that, and raw escapes in the transcript would render as
// garbage. Matches CSI sequences and OSC strings. Built from a string with
// explicit unicode escapes so no literal control byte lives in this file.
const ANSI = new RegExp(
  // Control characters are exactly what this matches -- that is the whole job.
  // eslint-disable-next-line no-control-regex
  "\\u001b\\[[0-9;?]*[ -/]*[@-~]|\\u001b\\][^\\u0007\\u001b]*(?:\\u0007|\\u001b\\\\)",
  "g",
);

export function stripAnsi(text: string): string {
  return text.replace(ANSI, "");
}

/** Turn a command id or role into something a human reads: `local.reset-thread`
 *  becomes "Reset thread". */
export function titleize(id: string): string {
  const last = id.split(".").pop() ?? id;
  const words = last.replace(/[-_]/g, " ").trim();
  return words.charAt(0).toUpperCase() + words.slice(1);
}
