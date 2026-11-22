import { createElement } from "react";
import { afterEach, describe, expect, it } from "vitest";
import { act, render, screen } from "@testing-library/react";
import { StoreProvider, initialState, navForPath, reducer, useStore } from "./store";
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

  it("claude_plan route a popstate on / restores a recorded view and a missing one stays Overview", () => {
    let s = reducer(initialState(), { type: "go", nav: "agents" });
    s = reducer(s, { type: "locate", pathname: "/", nav: "agents" });
    expect(s.nav).toBe("agents");
    expect(s.settingsSection).toBe("workspace");
    s = reducer(s, { type: "locate", pathname: "/" });
    expect(s.nav).toBe("overview");
    s = reducer(s, { type: "locate", pathname: "/", nav: null });
    expect(s.nav).toBe("overview");
    s = reducer(s, { type: "go", nav: "agents" });
    s = reducer(s, { type: "locate", pathname: CLAUDE_PLAN_PATH, nav: "agents" });
    expect(s.nav).toBe("settings");
    expect(s.settingsSection).toBe("claude-plan");
  });
});

function Probe() {
  const { state, dispatch } = useStore();
  return createElement(
    "div",
    null,
    createElement("div", { "data-testid": "nav" }, state.nav),
    createElement("div", { "data-testid": "section" }, state.settingsSection),
    createElement("button", { type: "button", onClick: () => dispatch({ type: "go", nav: "agents" }) }, "to-agents"),
    createElement("button", { type: "button", onClick: () => dispatch({ type: "go", nav: "settings" }) }, "to-settings"),
    createElement(
      "button",
      { type: "button", onClick: () => dispatch({ type: "settingsSection", section: "claude-plan" }) },
      "to-plan",
    ),
  );
}

// jsdom traverses history on two queued tasks. Wait them out inside act so the
// popstate update is flushed with the assertion.
async function traverse(direction: "back" | "forward") {
  await act(async () => {
    if (direction === "back") window.history.back();
    else window.history.forward();
    await new Promise((resolve) => setImmediate(resolve));
    await new Promise((resolve) => setImmediate(resolve));
  });
}

describe("claude_plan history", () => {
  afterEach(() => {
    window.history.replaceState(null, "", "/");
  });

  it("claude_plan history Back from Agents then Claude plan returns to Agents", async () => {
    window.history.replaceState(null, "", "/");
    render(createElement(StoreProvider, null, createElement(Probe)));
    expect(screen.getByTestId("nav")).toHaveTextContent("overview");

    await act(async () => {
      screen.getByRole("button", { name: "to-agents" }).click();
    });
    expect(screen.getByTestId("nav")).toHaveTextContent("agents");

    await act(async () => {
      screen.getByRole("button", { name: "to-plan" }).click();
    });
    expect(window.location.pathname).toBe(CLAUDE_PLAN_PATH);
    expect(screen.getByTestId("nav")).toHaveTextContent("settings");

    await traverse("back");
    expect(screen.getByTestId("nav")).toHaveTextContent("agents");
    expect(window.location.pathname).toBe("/");
  });

  it("claude_plan history Back from Agents returns to Claude plan and Forward reverses it", async () => {
    window.history.replaceState(null, "", "/");
    render(createElement(StoreProvider, null, createElement(Probe)));
    await act(async () => {
      screen.getByRole("button", { name: "to-plan" }).click();
    });
    expect(window.location.pathname).toBe(CLAUDE_PLAN_PATH);

    await act(async () => {
      screen.getByRole("button", { name: "to-agents" }).click();
    });
    expect(screen.getByTestId("nav")).toHaveTextContent("agents");
    expect(window.location.pathname).toBe("/");

    await traverse("back");
    expect(window.location.pathname).toBe(CLAUDE_PLAN_PATH);
    expect(screen.getByTestId("nav")).toHaveTextContent("settings");
    expect(screen.getByTestId("section")).toHaveTextContent("claude-plan");

    await traverse("forward");
    expect(screen.getByTestId("nav")).toHaveTextContent("agents");
    expect(window.location.pathname).toBe("/");
  });

  it("claude_plan history a load of / with no history state is Overview", () => {
    window.history.replaceState(null, "", "/");
    expect(window.history.state).toBeNull();
    expect(initialState().nav).toBe("overview");
    expect(initialState().settingsSection).toBe("workspace");
  });

  it("claude_plan history a reload of Agents shows Agents and Settings then Back stays there", async () => {
    window.history.replaceState({ nav: "agents" }, "", "/");
    const length = window.history.length;
    render(createElement(StoreProvider, null, createElement(Probe)));
    expect(window.history.length).toBe(length);
    expect(screen.getByTestId("nav")).toHaveTextContent("agents");
    expect(screen.getByTestId("section")).toHaveTextContent("workspace");
    expect(window.location.pathname).toBe("/");
    expect(window.history.state).toEqual({ nav: "agents" });

    await act(async () => {
      screen.getByRole("button", { name: "to-settings" }).click();
    });
    expect(screen.getByTestId("nav")).toHaveTextContent("settings");
    expect(window.location.pathname).toBe("/settings");

    await traverse("back");
    expect(screen.getByTestId("nav")).toHaveTextContent("agents");
    expect(window.location.pathname).toBe("/");
    expect(window.history.state).toEqual({ nav: "agents" });
  });
});
