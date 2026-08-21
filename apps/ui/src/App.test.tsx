import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { App } from "./App";
import { StoreProvider } from "./state/store";
import { WiredProvider } from "./state/wired";
import { getAgents, getConfig, listDeployments, type AgentOut } from "./api/client";

// The console is always backed by the live API, so App renders the wired shell.
// We mock the data layer so the tree resolves deterministically without a backend.
vi.mock("./api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./api/client")>();
  return {
    ...actual,
    getAgents: vi.fn(),
    getConfig: vi.fn(),
    listDeployments: vi.fn(),
  };
});

// [FIXTURE-ONLY] ADR-0116: AgentOut carries `channels` (plural); unused by any
// assertion in this file.
const AGENT: AgentOut = {
  id: "a1",
  name: "deal-desk",
  channels: [{ kind: "slack", address: "C0123ABCD" }],
  model: null,
  created_at: "2026-07-01T00:00:00Z",
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(getConfig).mockResolvedValue({ org_name: "Globex Corporation" });
  vi.mocked(listDeployments).mockResolvedValue([]);
});

function renderApp() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <StoreProvider>
        <WiredProvider>
          <App />
        </WiredProvider>
      </StoreProvider>
    </QueryClientProvider>,
  );
}

describe("App shell (wired)", () => {
  it("renders the seven nav items", () => {
    vi.mocked(getAgents).mockResolvedValue([]);
    renderApp();
    const nav = screen.getByRole("navigation");
    for (const label of ["Overview", "Agents", "Evals", "Observability", "Versions", "Connections", "Settings"]) {
      expect(within(nav).getByText(label)).toBeInTheDocument();
    }
  });

  it("shows onboarding for a fresh workspace with no agents", async () => {
    vi.mocked(getAgents).mockResolvedValue([]);
    renderApp();
    expect(await screen.findByText("Welcome to Curie")).toBeInTheDocument();
  });

  it("shows the configured workspace name from the live config", async () => {
    vi.mocked(getAgents).mockResolvedValue([AGENT]);
    renderApp();
    await waitFor(() => expect(screen.getAllByText("Globex Corporation").length).toBeGreaterThanOrEqual(1));
  });

  it("navigates to Agents and prompts to create the first agent", async () => {
    vi.mocked(getAgents).mockResolvedValue([]);
    const user = userEvent.setup();
    renderApp();
    await user.click(within(screen.getByRole("navigation")).getByText("Agents"));
    expect(await screen.findByText("Create your first agent")).toBeInTheDocument();
  });
});
