import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { PropsWithChildren } from "react";
import { StoreProvider } from "../../state/store";
import { WiredProvider } from "../../state/wired";
import { WiredWorkItems } from "./WiredWorkItems";
import {
  ApiError,
  getAgents,
  getWorkItem,
  listWorkItems,
  type AgentOut,
  type WorkItemOutcome,
} from "../../api/client";

// Mock only the data-layer calls; the real view and the real WiredProvider run
// against the stubs. State and cause strings come from the API and must be
// rendered verbatim (the console never re-derives them, #2577 D1).
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    getAgents: vi.fn(),
    getConfig: vi.fn().mockResolvedValue({}),
    listWorkItems: vi.fn(),
    getWorkItem: vi.fn(),
  };
});

const AGENTS: AgentOut[] = [
  { id: "ag-1", name: "factory", channels: [{ kind: "github", address: "acme-corp/acme-bot" }], model: null, created_at: "2026-09-01T00:00:00Z" },
  { id: "ag-2", name: "other-factory", channels: [], model: null, created_at: "2026-09-01T00:00:00Z" },
] as AgentOut[];

function outcome(overrides: Partial<WorkItemOutcome> = {}): WorkItemOutcome {
  return {
    id: "wi-1",
    agent_id: "ag-1",
    repo_full_name: "acme-corp/acme-bot",
    github_issue_number: 2577,
    issue_url: "https://github.com/acme-corp/acme-bot/issues/2577",
    cancelled_at: null,
    created_at: "2026-09-22T10:00:00Z",
    updated_at: "2026-09-22T10:05:00Z",
    objective: "Implement the admitted work item",
    objective_truncated: false,
    requester: "U0REQUEST1",
    state: "published",
    actionable_cause: "PR opened; zq-cause-verbatim",
    pr: { number: 123, url: "https://github.com/acme-corp/acme-bot/pull/123", status: "open" },
    publication: { status: "succeeded", revision_number: 1, approval_status: "approved" },
    correctness: { asserted: false, owner: "bundle" },
    ci: null,
    requests: [
      {
        sequence: 1,
        status: "completed",
        created_at: "2026-09-22T10:00:00Z",
        wait_deadline: "2026-09-23T10:00:00Z",
        started_at: "2026-09-22T10:01:00Z",
        execution_deadline: "2026-09-22T10:31:00Z",
        terminal_at: "2026-09-22T10:04:00Z",
        terminal_cause: "completed",
        termination_observation: null,
        capacity_deferrals: 0,
        last_deferral_reason: null,
      },
    ],
    ...overrides,
  } as WorkItemOutcome;
}

const WAITING = outcome({
  id: "wi-2",
  github_issue_number: 2578,
  issue_url: "https://github.com/acme-corp/acme-bot/issues/2578",
  state: "waiting",
  actionable_cause: "no capacity yet; zq-waiting-verbatim",
  pr: null,
  publication: null,
});

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
  return render(<WiredWorkItems />, { wrapper: wrapper() });
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(getAgents).mockResolvedValue(AGENTS);
});

describe("WiredWorkItems (#2577)", () => {
  it("renders one row per item with the API state, issue link, PR link and cause", async () => {
    vi.mocked(listWorkItems).mockResolvedValue({ items: [outcome(), WAITING], limit: 50, truncated: false });
    renderView();

    const rows = await screen.findAllByTestId("work-item-row");
    expect(rows).toHaveLength(2);

    const published = rows[0];
    expect(within(published).getByText("published")).toBeInTheDocument();
    expect(within(published).getByText(/zq-cause-verbatim/)).toBeInTheDocument();
    const issue = within(published).getByRole("link", { name: /acme-corp\/acme-bot#2577/ });
    expect(issue).toHaveAttribute("href", "https://github.com/acme-corp/acme-bot/issues/2577");
    expect(issue).toHaveAttribute("target", "_blank");
    expect(issue).toHaveAttribute("rel", expect.stringContaining("noreferrer"));
    const pr = within(published).getByRole("link", { name: /#?123/ });
    expect(pr).toHaveAttribute("href", "https://github.com/acme-corp/acme-bot/pull/123");

    const waiting = rows[1];
    expect(within(waiting).getByText("waiting")).toBeInTheDocument();
    expect(within(waiting).getByText(/zq-waiting-verbatim/)).toBeInTheDocument();
    expect(within(waiting).getByText("none")).toBeInTheDocument();
    expect(within(waiting).queryByRole("link", { name: /pull/ })).toBeNull();
  });

  it("renders the empty state, not an error, for an empty install", async () => {
    vi.mocked(listWorkItems).mockResolvedValue({ items: [], limit: 50, truncated: false });
    renderView();

    expect(await screen.findByText(/no factory work items/i)).toBeInTheDocument();
    expect(screen.queryAllByTestId("work-item-row")).toHaveLength(0);
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("opens the detail with CI state and reason and the correctness line", async () => {
    vi.mocked(listWorkItems).mockResolvedValue({ items: [outcome()], limit: 50, truncated: false });
    vi.mocked(getWorkItem).mockResolvedValue(
      outcome({
        ci: {
          state: "unavailable",
          reason: "github_forbidden",
          observed_at: "2026-09-22T10:06:00Z",
          head_sha: "1123456789abcdef0123456789abcdef01234567",
        },
      }),
    );
    renderView();

    await userEvent.click(await screen.findByTestId("work-item-row"));

    await waitFor(() => expect(getWorkItem).toHaveBeenCalledWith("wi-1"));
    const detail = await screen.findByTestId("work-item-detail");
    expect(detail).toHaveTextContent("unavailable");
    expect(detail).toHaveTextContent("github_forbidden");
    expect(detail).toHaveTextContent(/not asserted by the platform/i);
    expect(detail).toHaveTextContent(/owned by the bundle/i);
    expect(detail).toHaveTextContent("succeeded");
    // Request history is shown.
    expect(detail).toHaveTextContent("completed");
  });

  it("heads the detail with the issue and PR links, and says when there is no PR", async () => {
    vi.mocked(listWorkItems).mockResolvedValue({ items: [outcome(), WAITING], limit: 50, truncated: false });
    vi.mocked(getWorkItem).mockImplementation(async (id: string) => (id === "wi-1" ? outcome() : WAITING));
    renderView();

    const rows = await screen.findAllByTestId("work-item-row");
    await userEvent.click(rows[0]);
    let header = await screen.findByTestId("work-item-detail-header");
    expect(within(header).getByRole("link", { name: `acme-corp/acme-bot#${2577}` })).toHaveAttribute(
      "href",
      "https://github.com/acme-corp/acme-bot/issues/2577",
    );
    expect(within(header).getByRole("link", { name: /PR #123/ })).toHaveAttribute(
      "href",
      "https://github.com/acme-corp/acme-bot/pull/123",
    );

    await userEvent.click(rows[1]);
    await waitFor(() =>
      expect(within(screen.getByTestId("work-item-detail-header")).getByRole("link", { name: `acme-corp/acme-bot#${2578}` })).toBeInTheDocument(),
    );
    header = screen.getByTestId("work-item-detail-header");
    expect(within(header).queryByRole("link", { name: /PR #/ })).toBeNull();
    expect(header).toHaveTextContent("no PR");
  });

  it("says the item no longer exists when the detail is a 404", async () => {
    vi.mocked(listWorkItems).mockResolvedValue({ items: [outcome()], limit: 50, truncated: false });
    vi.mocked(getWorkItem).mockRejectedValue(new ApiError(404, "not_found"));
    renderView();

    await userEvent.click(await screen.findByTestId("work-item-row"));

    expect(await screen.findByText(/no longer exists/i)).toBeInTheDocument();
  });

  it("renders an error notice when the list fails", async () => {
    vi.mocked(listWorkItems).mockRejectedValue(new ApiError(503, "zq-list-boom"));
    renderView();

    expect(await screen.findByText(/zq-list-boom/)).toBeInTheDocument();
    expect(screen.queryByText(/no factory work items/i)).toBeNull();
  });

  it("filters by agent through the API, not client-side", async () => {
    vi.mocked(listWorkItems).mockResolvedValue({ items: [outcome()], limit: 50, truncated: false });
    renderView();
    await screen.findAllByTestId("work-item-row");

    const select = await screen.findByLabelText(/agent/i);
    await waitFor(() => expect(within(select).getByRole("option", { name: "other-factory" })).toBeInTheDocument());
    await userEvent.selectOptions(select, "ag-2");

    await waitFor(() =>
      expect(listWorkItems).toHaveBeenLastCalledWith(expect.objectContaining({ agentId: "ag-2" })),
    );
  });

  // --- review round 1 regressions ---------------------------------------------

  it("shows a visible truncation notice naming the limit when the API truncates", async () => {
    vi.mocked(listWorkItems).mockResolvedValue({ items: [outcome()], limit: 50, truncated: true });
    renderView();

    await screen.findAllByTestId("work-item-row");
    const notice = await screen.findByTestId("work-items-truncated");
    expect(notice).toBeVisible();
    expect(notice).toHaveTextContent("50");
  });

  it("does not keep showing agent A's detail after switching the filter to agent B", async () => {
    vi.mocked(listWorkItems).mockImplementation(async (opts?: { agentId?: string }) =>
      opts?.agentId === "ag-2"
        ? { items: [], limit: 50, truncated: false }
        : { items: [outcome()], limit: 50, truncated: false },
    );
    vi.mocked(getWorkItem).mockImplementation(async (_id: string, scope?: { agentId?: string }) => {
      if (scope?.agentId && scope.agentId !== "ag-1") throw new ApiError(404, "not_found");
      return outcome({ actionable_cause: "zq-agent-a-detail" });
    });
    renderView();

    await userEvent.click(await screen.findByTestId("work-item-row"));
    const detail = await screen.findByTestId("work-item-detail");
    expect(detail).toHaveTextContent("zq-agent-a-detail");

    const select = await screen.findByLabelText(/agent/i);
    await waitFor(() => expect(within(select).getByRole("option", { name: "other-factory" })).toBeInTheDocument());
    await userEvent.selectOptions(select, "ag-2");

    await screen.findByText(/no factory work items/i);
    await waitFor(() => expect(screen.queryByTestId("work-item-detail")).toBeNull());
    expect(screen.queryByText(/zq-agent-a-detail/)).toBeNull();
  });

  it("refreshes the list and the selected detail, and shows when CI was observed", async () => {
    vi.mocked(listWorkItems).mockResolvedValue({ items: [outcome()], limit: 50, truncated: false });
    vi.mocked(getWorkItem)
      .mockResolvedValueOnce(
        outcome({
          ci: { state: "pending", reason: null, observed_at: "2026-09-22T10:06:00Z", head_sha: "1123456789abcdef0123456789abcdef01234567" },
        }),
      )
      .mockResolvedValue(
        outcome({
          ci: { state: "passing", reason: null, observed_at: "2026-09-22T10:09:00Z", head_sha: "1123456789abcdef0123456789abcdef01234567" },
        }),
      );
    renderView();

    await userEvent.click(await screen.findByTestId("work-item-row"));
    const detail = await screen.findByTestId("work-item-detail");
    expect(detail).toHaveTextContent("pending");
    expect(detail.querySelector('time[datetime="2026-09-22T10:06:00Z"]')).not.toBeNull();

    const listCalls = vi.mocked(listWorkItems).mock.calls.length;
    const detailCalls = vi.mocked(getWorkItem).mock.calls.length;
    await userEvent.click(screen.getByRole("button", { name: /refresh/i }));

    await waitFor(() => expect(vi.mocked(listWorkItems).mock.calls.length).toBeGreaterThan(listCalls));
    await waitFor(() => expect(vi.mocked(getWorkItem).mock.calls.length).toBeGreaterThan(detailCalls));
    await waitFor(() => expect(screen.getByTestId("work-item-detail")).toHaveTextContent("passing"));
    expect(
      screen.getByTestId("work-item-detail").querySelector('time[datetime="2026-09-22T10:09:00Z"]'),
    ).not.toBeNull();
  });

  it("offers the CLI equivalent of this view", async () => {
    vi.mocked(listWorkItems).mockResolvedValue({ items: [], limit: 50, truncated: false });
    renderView();

    await screen.findByText(/no factory work items/i);
    expect(
      screen.getByRole("button", { name: /copy command: curie (local|cluster) work-items/i }),
    ).toBeInTheDocument();
  });
});
