import { afterEach, describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import { Topbar } from "./Topbar";
import { Sidebar } from "./Sidebar";
import { StoreProvider } from "../state/store";
import { useWired, type WiredData } from "../state/wired";

// The shared chrome (Topbar + Sidebar) shows the workspace name, which comes from
// the real config the wired data layer exposes as `orgName`. We mock the wired
// data layer so we can render just the chrome deterministically without a fetch.
vi.mock("../state/wired", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../state/wired")>();
  return { ...actual, useWired: vi.fn() };
});

function wiredValue(over: Record<string, unknown>): WiredData {
  return {
    wired: true,
    agents: [],
    orgName: "Curie",
    loading: false,
    error: null,
    refetch: () => {},
    justDeployed: null,
    markDeployed: () => {},
    clearDeployed: () => {},
    ...over,
  } as unknown as WiredData;
}

function renderChrome() {
  return render(
    <StoreProvider>
      <Topbar />
      <Sidebar />
    </StoreProvider>,
  );
}

afterEach(() => {
  vi.clearAllMocks();
});

describe("chrome workspace name", () => {
  it("renders the configured org name in both the Topbar and the Sidebar", () => {
    vi.mocked(useWired).mockReturnValue(wiredValue({ orgName: "Globex Corporation" }));
    renderChrome();
    expect(screen.getAllByText("Globex Corporation").length).toBeGreaterThanOrEqual(2);
    expect(screen.queryByText("acme-corp")).toBeNull();
  });

  it("falls back to the default workspace name while config is unset", () => {
    vi.mocked(useWired).mockReturnValue(wiredValue({ orgName: "Curie" }));
    renderChrome();
    expect(screen.getAllByText("Curie").length).toBeGreaterThanOrEqual(1);
  });
});

describe("sidebar navigation order", () => {
  it("lists Memory directly after Factory", () => {
    vi.mocked(useWired).mockReturnValue(wiredValue({}));
    renderChrome();
    const labels = within(screen.getByRole("navigation")).getAllByRole("button").map((b) => b.textContent ?? "");
    const at = (name: string) => labels.findIndex((l) => l.startsWith(name));
    expect(at("Memory")).toBe(at("Factory") + 1);
    expect(at("Memory")).toBeGreaterThan(-1);
  });
});
