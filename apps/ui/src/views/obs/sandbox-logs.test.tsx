import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { useEffect } from "react";
import { RealTraceDetail } from "./RealTraces";
import { RealLogs } from "./RealLogs";
import { StoreProvider, useStore } from "../../state/store";
import { getTrace, listRunnerPods, type TraceTree, type RunnerPods } from "../../api/client";

// Mock only the data-layer calls; keep the real ApiError so RealLogs' error
// branching (instanceof ApiError) is preserved.
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return { ...actual, getTrace: vi.fn(), listRunnerPods: vi.fn() };
});

const TRACE: TraceTree = {
  trace: { id: "tr-1", name: "curie-run:agent-x-thread-1" },
  tree: [{ id: "root", type: "SPAN", name: "agent.run", model: null, startTime: "1", usageDetails: null, children: [] }],
  sandbox_id: "sbx-42",
};

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(getTrace).mockResolvedValue(TRACE);
  vi.mocked(listRunnerPods).mockResolvedValue({ namespace: "curie", pods: ["pod-a"] } as RunnerPods);
});

function renderWired(ui: React.ReactNode) {
  // RealTraceDetail consumes the react-query useTrace hook, so a QueryClientProvider
  // is required (retry off so results resolve on the first response).
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <StoreProvider>{ui}</StoreProvider>
    </QueryClientProvider>,
  );
}

// Surfaces the nav-relevant store state so a test can assert what openLogs did.
function NavProbe() {
  const { state } = useStore();
  return <div data-testid="nav-probe">{`${state.nav}:${state.obsTab}:${state.logsPod ?? ""}`}</div>;
}

describe("RealTraceDetail — view sandbox logs (issue #16)", () => {
  it("renders an observation with omitted optional children", async () => {
    // ObservationNode requires only id and type in apps/api/openapi.json.
    vi.mocked(getTrace).mockResolvedValue({
      trace: { id: "tr-1", name: "example trace" },
      tree: [{ id: "root", type: "SPAN" }],
    } as TraceTree);

    function Harness() {
      const { dispatch } = useStore();
      useEffect(() => {
        dispatch({ type: "openTrace", id: "tr-1" });
      }, [dispatch]);
      return <RealTraceDetail />;
    }

    renderWired(<Harness />);
    expect(await screen.findByTestId("span-tree")).toBeInTheDocument();
    expect(screen.getAllByText("SPAN")).toHaveLength(2);
  });

  it("shows the control when a typed sandbox id is present and jumps to the logs tab prefilled", async () => {
    const user = userEvent.setup();

    function Harness() {
      const { dispatch } = useStore();
      useEffect(() => {
        dispatch({ type: "openTrace", id: "tr-1" });
      }, [dispatch]);
      return (
        <>
          <RealTraceDetail />
          <NavProbe />
        </>
      );
    }

    renderWired(<Harness />);

    // The "Served by sandbox" row and the jump control appear once the trace loads.
    expect(await screen.findByTestId("trace-sandbox")).toHaveTextContent("sbx-42");
    const btn = await screen.findByTestId("view-sandbox-logs");

    await user.click(btn);

    // The store navigated to the Logs tab preselected to the serving sandbox.
    await waitFor(() =>
      expect(screen.getByTestId("nav-probe")).toHaveTextContent("observability:logs:sbx-42"),
    );
  });

  it("does not render the control when no sandbox id is present", async () => {
    vi.mocked(getTrace).mockResolvedValue({ ...TRACE, sandbox_id: null, trace: { id: "tr-1", name: "n" } });

    function Harness() {
      const { dispatch } = useStore();
      useEffect(() => {
        dispatch({ type: "openTrace", id: "tr-1" });
      }, [dispatch]);
      return <RealTraceDetail />;
    }

    renderWired(<Harness />);
    // The span tree renders (trace loaded) but the sandbox control is absent.
    expect(await screen.findByTestId("span-tree")).toBeInTheDocument();
    expect(screen.queryByTestId("view-sandbox-logs")).toBeNull();
  });
});

describe("RealLogs — sandbox prefill (issue #16)", () => {
  it("preselects the prefilled sandbox id as the pod, even if not in the fetched list", async () => {
    function Harness() {
      const { dispatch } = useStore();
      useEffect(() => {
        dispatch({ type: "openLogs", sandboxId: "sbx-42" });
      }, [dispatch]);
      return <RealLogs />;
    }

    renderWired(<Harness />);

    // The pod dropdown is preselected to the prefilled sandbox id, and it exists
    // as an option even though the cluster only returned "pod-a".
    const select = (await screen.findByTestId("logs-pod-select")) as HTMLSelectElement;
    await waitFor(() => expect(select.value).toBe("sbx-42"));
    expect(Array.from(select.options).map((o) => o.value)).toContain("sbx-42");
  });
});
