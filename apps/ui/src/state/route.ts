import type { Nav } from "./types";

// The console is not a router. Only Settings has paths of its own: the
// workspace section at /settings and the Claude plan section at the frozen
// /settings/claude-plan, which other tickets link to. Every other URL opens
// Overview.

export type SettingsSection = "workspace" | "claude-plan";

export const SETTINGS_PATH = "/settings";
export const CLAUDE_PLAN_PATH = "/settings/claude-plan";

export interface Route {
  nav: Nav;
  settingsSection: SettingsSection;
}

export function routeForPath(pathname: string): Route {
  if (pathname === CLAUDE_PLAN_PATH) return { nav: "settings", settingsSection: "claude-plan" };
  if (pathname === SETTINGS_PATH) return { nav: "settings", settingsSection: "workspace" };
  return { nav: "overview", settingsSection: "workspace" };
}

// The path that shows a state. Views other than Settings share "/", since the
// console does not give them URLs.
export function pathForRoute(nav: Nav, section: SettingsSection): string {
  if (nav !== "settings") return "/";
  return section === "claude-plan" ? CLAUDE_PLAN_PATH : SETTINGS_PATH;
}
