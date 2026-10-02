// Presentation only. Authorization and machine payloads keep exact identifiers.
const native: Record<string, string> = { Bash: "shell request", Skill: "instruction request", Read: "read file", Write: "write file", Edit: "edit file", MultiEdit: "edit files", Glob: "find files", Grep: "search files", WebFetch: "fetch web page", WebSearch: "search web" };
export function actionLabel(value: unknown): string {
  if (typeof value !== "string") return "action";
  let name = value;
  if (!/^[\p{L}\p{N}_-]+$/u.test(name)) return "action";
  if (name.startsWith("mcp__")) {
    const parts = name.split("__");
    if (parts.length < 3 || parts.some(p => !p)) return "action";
    name = parts[parts.length - 1];
  } else if (Object.prototype.hasOwnProperty.call(native, name)) return native[name];
  return name.replace(/([A-Z]+)([A-Z][a-z])/g, "$1 $2").replace(/([a-z0-9])([A-Z])/g, "$1 $2").replace(/[_-]+/g, " ").toLowerCase().trim() || "action";
}
function displayValue(value: unknown): string {
  if (value === null) return "none";
  // Nested property names and JSON types are requested data.
  if (typeof value === "object") return JSON.stringify(value);
  if (typeof value === "boolean") return value ? "yes" : "no";
  return String(value) || "(empty)";
}
function caption(key: string): string {
  const label = actionLabel(key);
  return label.charAt(0).toUpperCase() + label.slice(1);
}
export function approvalSummary(summary: string): string {
  const prefix = "Tool call awaiting approval: ";
  if (!summary.startsWith(prefix)) return summary.replace(/(?<![\w./-])mcp__[\w-]+(?![\w./-])/g, actionLabel);
  const rest = summary.slice(prefix.length);
  const boundary = rest.indexOf(" ");
  const tool = boundary < 0 ? rest : rest.slice(0, boundary);
  let details = "Details are incomplete; review the original request before approving.";
  try {
    const args: unknown = JSON.parse(rest.slice(boundary + 1));
    if (args !== null && typeof args === "object" && !Array.isArray(args)) {
      details = Object.entries(args).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => `${caption(k)}: ${displayValue(v)}`).join("; ");
    }
  } catch { /* Older truncated summaries require review of the original request. */ }
  return `Approve ${actionLabel(tool)}.${details ? " " + details : ""}`;
}
