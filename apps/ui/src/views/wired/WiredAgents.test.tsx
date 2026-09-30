import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { PropsWithChildren } from "react";
import { StoreProvider } from "../../state/store";
import { WiredProvider } from "../../state/wired";
import { WiredAgents } from "./WiredAgents";
import { getAgents, getMetricsSummary, listAllDeployments, type AgentOut, type MetricsSummary } from "../../api/client";

// Mock only the data-layer calls; the real view and the real WiredProvider run
// against the stubs.
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    getAgents: vi.fn(),
    getConfig: vi.fn().mockResolvedValue({}),
    getMetricsSummary: vi.fn(),
    listAllDeployments: vi.fn(),
  };
});

const AGENTS: AgentOut[] = [
  { id: "ag-1", name: "factory", channels: [{ kind: "github", address: "acme-corp/acme-bot" }], model: null, created_at: "2026-09-01T00:00:00Z" },
];

function summary(overrides: Partial<MetricsSummary> = {}): MetricsSummary {
  return {
    start: "2026-09-18T00:00:00Z",
    end: "2026-09-25T00:00:00Z",
    runs: 12,
    latency_p95_ms: 500,
    tokens: 45000,
    cost_usd: 3.4567,
    error_rate: 0,
    ...overrides,
  };
}

function wrapper() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>
        <StoreProvider>
          <WiredProvider>{children}</WiredProvider>
        </StoreProvider>
      </QueryClientProvider>
    );
  };
}

function renderView() {
  return render(<WiredAgents />, { wrapper: wrapper() });
}

beforeEach(() => {
  vi.mocked(getAgents).mockResolvedValue(AGENTS);
  vi.mocked(listAllDeployments).mockResolvedValue([]);
});

describe("WiredAgents metrics line", () => {
  it("requests the summary scoped to this agent with no environment param and renders it", async () => {
    vi.mocked(getMetricsSummary).mockResolvedValue(summary());
    renderView();

    await waitFor(() => expect(getMetricsSummary).toHaveBeenCalled());
    expect(getMetricsSummary).toHaveBeenCalledWith({ agent: "agent-ag-1" });

    await waitFor(() => expect(screen.getByText(/12 runs/)).toBeTruthy());
    expect(screen.getByText(/45,000 tokens/)).toBeTruthy();
    expect(screen.getByText(/\$3\.46/)).toBeTruthy();
    expect(screen.getByText(/last 7d/)).toBeTruthy();
  });

  it("shows an unknown cost, not $0.00, when the model has no price row", async () => {
    vi.mocked(getMetricsSummary).mockResolvedValue(summary({ cost_usd: 0, cost_known: false }));
    renderView();

    await waitFor(() => expect(screen.getByText(/12 runs · 45,000 tokens · — /)).toBeTruthy());
    expect(screen.queryByText(/\$0\.00/)).toBeNull();
  });

  it("shows a neutral dash on a fetch error, never 0 runs", async () => {
    vi.mocked(getMetricsSummary).mockRejectedValue(new Error("zq-metrics-boom"));
    renderView();

    await waitFor(() => expect(getMetricsSummary).toHaveBeenCalled());
    await waitFor(() => expect(screen.getByText(/— runs · — tokens · —/)).toBeTruthy());
    expect(screen.queryByText(/0 runs/)).toBeNull();
  });
});
