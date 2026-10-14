import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { PropsWithChildren } from "react";
import { StoreProvider } from "../../state/store";
import { WiredEvals } from "./WiredEvals";
import { getEvalMatrix, type EvalMatrix } from "../../api/client";

// Mock the eval-matrix data-layer call only; the real useEvalMatrix hook runs
// against the stub so the component + hook wiring is exercised end to end.
vi.mock("../../api/client", async (importActual) => {
  const actual = await importActual<typeof import("../../api/client")>();
  return {
    ...actual,
    getEvalMatrix: vi.fn(),
  };
});

const MATRIX: EvalMatrix = {
  suite: "default",
  versions: ["abc1234def", "def5678abc"],
  cases: ["approver-from-policy", "deal-data-not-slack"],
  rows: [
    {
      case_id: "approver-from-policy",
      cells: [
        { version: "abc1234def", status: "pass", model: "claude-sonnet-4.5" },
        { version: "def5678abc", status: "pass", model: "claude-sonnet-4.5" },
      ],
    },
    {
      case_id: "deal-data-not-slack",
      cells: [
        { version: "abc1234def", status: "fail", model: "claude-sonnet-4.5" },
        { version: "def5678abc", status: "missing", model: null },
      ],
    },
  ],
  models: ["claude-sonnet-4.5", "fake"],
  model_summaries: [
    { model: "claude-sonnet-4.5", passed: 3, total: 4, cost_usd: 0.0123, plumbing: 0, completed: 4 },
    { model: "fake", passed: 0, total: 2, cost_usd: null, plumbing: 1, completed: 0 },
  ],
};

// A fresh client per render with retry off, mirroring main.tsx, so the wired
// hook resolves on the first response instead of retrying.
function wrapper() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={client}>
        <StoreProvider>{children}</StoreProvider>
      </QueryClientProvider>
    );
  };
}

function renderView() {
  return render(<WiredEvals />, { wrapper: wrapper() });
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("WiredEvals (#868)", () => {
  it("renders the grid when optional model summaries are omitted", async () => {
    // apps/api/openapi.json requires only suite, versions, cases, and rows.
    vi.mocked(getEvalMatrix).mockResolvedValue({
      suite: MATRIX.suite,
      versions: MATRIX.versions,
      cases: MATRIX.cases,
      rows: MATRIX.rows,
    } as EvalMatrix);
    renderView();

    expect(await screen.findAllByTestId("matrix-row")).toHaveLength(2);
    expect(screen.queryByTestId("model-summary-row")).toBeNull();
  });

  it("uses declared defaults for omitted completion and plumbing counters", async () => {
    // EvalModelSummary defaults completed and plumbing to zero in openapi.json.
    vi.mocked(getEvalMatrix).mockResolvedValue({
      ...MATRIX,
      model_summaries: [{ model: "example-model", passed: 0, total: 2 }],
    } as EvalMatrix);
    renderView();

    const row = await screen.findByTestId("model-summary-row");
    expect(within(row).getByText("never ran")).toBeInTheDocument();
    expect(within(row).getAllByText("0/2")).toHaveLength(2);
    expect(within(row).queryByText(/not graded/)).toBeNull();
  });

  it("renders the matrix grid: one row per case, cells per version", async () => {
    vi.mocked(getEvalMatrix).mockResolvedValue(MATRIX);
    renderView();

    // Reads the matrix from the live API for the default suite.
    await waitFor(() => expect(getEvalMatrix).toHaveBeenCalledWith("default", 5));

    const rows = await screen.findAllByTestId("matrix-row");
    expect(rows).toHaveLength(2);
    expect(within(rows[0]).getByText("approver-from-policy")).toBeInTheDocument();
    // Version columns render short shas in the header.
    const header = screen.getByTestId("matrix-header");
    expect(within(header).getByText("abc1234")).toBeInTheDocument();
    expect(within(header).getByText("def5678")).toBeInTheDocument();
  });

  it("distinguishes pass / fail / missing cells by status", async () => {
    vi.mocked(getEvalMatrix).mockResolvedValue(MATRIX);
    renderView();

    const rows = await screen.findAllByTestId("matrix-row");
    const failRow = rows[1];
    const cells = within(failRow).getAllByTestId("eval-cell");
    expect(cells[0]).toHaveAttribute("data-status", "fail");
    expect(cells[1]).toHaveAttribute("data-status", "missing");
    const passCells = within(rows[0]).getAllByTestId("eval-cell");
    expect(passCells[0]).toHaveAttribute("data-status", "pass");
  });

  it("renders the per-model rollup, flagging a model that never completed", async () => {
    vi.mocked(getEvalMatrix).mockResolvedValue(MATRIX);
    renderView();

    const modelRows = await screen.findAllByTestId("model-summary-row");
    expect(modelRows).toHaveLength(2);
    // Real model: pass-rate + passed/total + cost.
    expect(within(modelRows[0]).getByText("claude-sonnet-4.5")).toBeInTheDocument();
    expect(within(modelRows[0]).getByText("75%")).toBeInTheDocument();
    expect(within(modelRows[0]).getByText("$0.0123")).toBeInTheDocument();
    // The fake tier has total > 0 but completed 0: flagged as never-run, not 0%.
    expect(within(modelRows[1]).getByText("never ran")).toBeInTheDocument();
    expect(within(modelRows[1]).getByText(/not graded/)).toBeInTheDocument();
  });

  it("refetches when the suite is changed", async () => {
    vi.mocked(getEvalMatrix).mockResolvedValue(MATRIX);
    renderView();
    await waitFor(() => expect(getEvalMatrix).toHaveBeenCalledWith("default", 5));

    const input = screen.getByTestId("eval-suite-input");
    await userEvent.clear(input);
    await userEvent.type(input, "deal-desk{Enter}");

    await waitFor(() => expect(getEvalMatrix).toHaveBeenCalledWith("deal-desk", 5));
  });

  it("shows an honest empty state for a suite with no runs", async () => {
    vi.mocked(getEvalMatrix).mockResolvedValue({
      ...MATRIX,
      versions: [],
      cases: [],
      rows: [],
      model_summaries: [],
    });
    renderView();
    expect(await screen.findByText(/No eval runs yet/i)).toBeInTheDocument();
  });

  it("surfaces a load error without leaking demo data", async () => {
    vi.mocked(getEvalMatrix).mockRejectedValue(new Error("boom"));
    renderView();
    expect(await screen.findByText(/eval matrix is not available/i)).toBeInTheDocument();
    expect(screen.getByText(/boom/)).toBeInTheDocument();
  });
});
