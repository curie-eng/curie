import { describe, expect, it } from "vitest";
import { initialState, navForPath, reducer } from "./store";
import { CLAUDE_PLAN_PATH, pathForRoute, routeForPath } from "./route";

describe("claude_plan route", () => {
  it("claude_plan route reads the two Settings paths and nothing else", () => {
    expect(routeForPath(CLAUDE_PLAN_PATH)).toEqual({ nav: "settings", settingsSection: "claude-plan" });
    expect(routeForPath("/settings")).toEqual({ nav: "settings", settingsSection: "workspace" });
    for (const other of ["/", "/agents", "/settings/", "/settings/claude-plan/extra", "/settings/claude-plan/"]) {
      expect(navForPath(other), other).toBe("overview");
    }
  });

  it("claude_plan route freezes the path other tickets link to", () => {
    expect(CLAUDE_PLAN_PATH).toBe("/settings/claude-plan");
    expect(pathForRoute("settings", "claude-plan")).toBe("/settings/claude-plan");
    expect(pathForRoute("settings", "workspace")).toBe("/settings");
    expect(pathForRoute("agents", "claude-plan")).toBe("/");
  });

  it("claude_plan route first paint opens the section the path names", () => {
    window.history.pushState({}, "", CLAUDE_PLAN_PATH);
    expect(initialState().settingsSection).toBe("claude-plan");
    window.history.pushState({}, "", "/");
    expect(initialState().nav).toBe("overview");
  });

  it("claude_plan route section and popstate actions move the view", () => {
    let s = reducer(initialState(), { type: "settingsSection", section: "claude-plan" });
    expect(s.nav).toBe("settings");
    expect(s.settingsSection).toBe("claude-plan");
    s = reducer(s, { type: "locate", pathname: "/" });
    expect(s.nav).toBe("overview");
    s = reducer(s, { type: "locate", pathname: CLAUDE_PLAN_PATH });
    expect(s.nav).toBe("settings");
    expect(s.settingsSection).toBe("claude-plan");
  });
});
