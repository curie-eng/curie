import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import type { PropsWithChildren } from "react";
import { StoreProvider } from "../../state/store";
import { WiredProvider } from "../../state/wired";
import { WiredFactory } from "./WiredFactory";
import { Sidebar } from "../../components/Sidebar";
import { WiredWorkItems } from "./WiredWorkItems";
import * as client from "../../api/client";
import { ApiError, getAgents, getWorkItem, listWorkItems, type AgentOut, type WorkItemOutcome } from "../../api/client";

// Factory view (#4102). Mock only the data-layer calls; the real view and the
// real WiredProvider run against the stubs. State and cause strings come from
// the API and are rendered verbatim (the console never re-derives them, #2577).
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    getAgents: vi.fn(),
    getConfig: vi.fn().mockResolvedValue({}),
    listWorkItems: vi.fn(),
    getWorkItem: vi.fn(),
    getWorkItemUsage: vi.fn(),
  };
});

// getWorkItemUsage is added to client.ts by this change; reach it through the
// module namespace so the test compiles against the mocked module either way.
const getWorkItemUsage = (client as unknown as { getWorkItemUsage: ReturnType<typeof vi.fn> }).getWorkItemUsage;

type StageState = "done" | "current" | "redo" | "blocked" | "pending";
type Stage = { id: string; label: string; state: StageState; round_label: string | null };
type Progress = { current: string | null; note: string | null; stages: Stage[] } | null;
type State = WorkItemOutcome["state"];

const AGENTS: AgentOut[] = [
  { id: "ag-1", name: "factory", channels: [{ kind: "github", address: "acme/payments" }], model: null, created_at: "2026-09-01T00:00:00Z" },
] as AgentOut[];

const HOUR = 3_600_000;
const DAY = 24 * HOUR;

function iso(msAgo: number): string {
  return new Date(Date.now() - msAgo).toISOString();
}

const STAGES: Stage[] = [
  { id: "plan", label: "Plan", state: "done", round_label: null },
  { id: "implement", label: "Implement", state: "current", round_label: "round 2 of 3" },
  { id: "review", label: "Review", state: "redo", round_label: null },
  { id: "publish", label: "Publish", state: "pending", round_label: null },
];

const PROGRESS: Progress = { current: "implement", note: "zq-latest-note: fixing the retry jitter", stages: STAGES };

function outcome(
  overrides: Partial<Omit<WorkItemOutcome, "state">> & { state?: State; title?: string | null; progress?: Progress } = {},
): WorkItemOutcome {
  const n = overrides.github_issue_number ?? 398;
  return {
    id: "wi-1",
    agent_id: "ag-1",
    repo_full_name: "acme/payments",
    github_issue_number: n,
    issue_url: `https://github.com/acme/payments/issues/${n}`,
    cancelled_at: null,
    created_at: iso(2 * HOUR),
    updated_at: iso(HOUR),
    objective: "Implement the admitted work item",
    objective_truncated: false,
    requester: "U0REQUEST1",
    state: "running",
    actionable_cause: "zq-cause-verbatim",
    title: "Retry webhook delivery with exponential backoff",
    progress: null,
    pr: null,
    publication: null,
    correctness: { asserted: false, owner: "bundle" },
    ci: null,
    requests: [
      {
        sequence: 1,
        status: "running",
        created_at: iso(2 * HOUR),
        wait_deadline: null,
        started_at: iso(2 * HOUR),
        execution_deadline: null,
        terminal_at: null,
        terminal_cause: null,
        termination_observation: null,
        capacity_deferrals: 0,
        last_deferral_reason: null,
      },
    ],
    ...overrides,
  } as unknown as WorkItemOutcome;
}

function usage(overrides: Record<string, unknown> = {}) {
  return {
    work_item_id: "wi-1",
    total_tokens: 182_000,
    estimated_cost_usd: "1.23",
    cost_complete: true,
    requests_without_usage: 0,
    roles: {
      implementer: { tokens: 150_000, estimated_cost_usd: "1.00", cost_complete: true },
      reviewer: { tokens: 32_000, estimated_cost_usd: "0.23", cost_complete: true },
    },
    models: [
      {
        model: "zq-model-implementer",
        role: "implementer",
        input_tokens: 120_000,
        cached_input_tokens: 10_000,
        cache_write_tokens: 5_000,
        output_tokens: 15_000,
        estimated_cost_usd: "1.00",
      },
      {
        model: "zq-model-reviewer",
        role: "reviewer",
        input_tokens: 30_000,
        cached_input_tokens: 0,
        cache_write_tokens: 0,
        output_tokens: 2_000,
        estimated_cost_usd: "0.23",
      },
    ],
    requests: [{ request_id: "req-1", tokens: 182_000, estimated_cost_usd: "1.23" }],
    price_sources: [{ source: "litellm", as_of: "2026-10-01T00:00:00Z" }],
    pr_number: null,
    pr_url: null,
    ...overrides,
  };
}

function list(items: WorkItemOutcome[]) {
  return { items, limit: 200, truncated: false };
}

function wrapper() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return function Wrapper({ children }: PropsWithChildren) {
    return (
      <QueryClientProvider client={queryClient}>
        <StoreProvider>
          <WiredProvider>{children}</WiredProvider>
        </StoreProvider>
      </QueryClientProvider>
    );
  };
}

function renderView() {
  return render(<WiredFactory />, { wrapper: wrapper() });
}

const LANE_IDS = ["queued", "running", "needs-you", "shipped"] as const;
type Lane = (typeof LANE_IDS)[number];

// Decision 5: every value of the WorkItemOutcomeOut state enum, and its lane.
// Typed as Record<State, ...> so a new enum value fails typecheck until it is
// placed here.
const STATE_LANE: Record<State, Lane | null> = {
  queued: "queued",
  waiting: "queued",
  running: "running",
  cancellation_requested: "running",
  publishing: "running",
  failed: "needs-you",
  expired: "needs-you",
  awaiting_approval: "needs-you",
  completed_unpublished: "needs-you",
  published: "shipped",
  cancelled: null,
};

function lane(id: Lane) {
  return screen.getByTestId(`factory-lane-${id}`);
}

function cardsIn(id: Lane) {
  return within(lane(id)).queryAllByTestId("factory-card");
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(getAgents).mockResolvedValue(AGENTS);
  getWorkItemUsage.mockResolvedValue(usage());
});

afterEach(() => {
  vi.useRealTimers();
});

describe("WiredFactory (#4102)", () => {
  it("adds Factory to the sidebar directly after Work items, which stays", () => {
    vi.mocked(listWorkItems).mockResolvedValue(list([]));
    render(<Sidebar />, { wrapper: wrapper() });
    const labels = within(screen.getByRole("navigation"))
      .getAllByRole("button")
      .map((button) => button.textContent?.trim());
    const workItems = labels.indexOf("Work items");
    expect(workItems).toBeGreaterThanOrEqual(0);
    expect(labels[workItems + 1]).toBe("Factory");
  });

  it("heads the view Factory and offers the CLI equivalent", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(list([]));
    renderView();

    expect(await screen.findByText("Factory", { exact: true })).toBeInTheDocument();
    expect(
      await screen.findByRole("button", { name: /copy command: curie (local|cluster) work-items/i }),
    ).toBeInTheDocument();
  });

  it("places every state enum value in its Decision 5 lane and leaves cancelled in none", async () => {
    const states = Object.keys(STATE_LANE) as State[];
    const items = states.map((state, i) =>
      outcome({
        id: `wi-${state}`,
        state,
        github_issue_number: 100 + i,
        title: `zq-title-${state}`,
        actionable_cause: `zq-cause-${state}`,
      }),
    );
    vi.mocked(listWorkItems).mockResolvedValue(list(items));
    renderView();

    await screen.findAllByTestId("factory-card");
    for (const state of states) {
      const expected = STATE_LANE[state];
      for (const id of LANE_IDS) {
        const inLane = cardsIn(id).some((card) => card.textContent?.includes(`zq-title-${state}`));
        expect(inLane, `${state} in lane ${id}`).toBe(id === expected);
      }
      if (expected) {
        const card = cardsIn(expected).find((c) => c.textContent?.includes(`zq-title-${state}`))!;
        // The state pill carries the API string verbatim.
        expect(within(card).getByText(state, { exact: true })).toBeInTheDocument();
      }
    }
    // cancelled is in no lane: total cards is every state but one.
    expect(screen.getAllByTestId("factory-card")).toHaveLength(states.length - 1);
    expect(screen.queryByText("zq-title-cancelled")).toBeNull();
    expect(screen.getByTestId("factory-cancelled")).toHaveTextContent("1 cancelled");
  });

  it("titles each lane and shows its count", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(
      list([
        outcome({ id: "q1", state: "queued", github_issue_number: 1 }),
        outcome({ id: "q2", state: "waiting", github_issue_number: 2 }),
        outcome({ id: "q3", state: "queued", github_issue_number: 3 }),
        outcome({ id: "r1", state: "running", github_issue_number: 4 }),
        outcome({ id: "n1", state: "failed", github_issue_number: 5 }),
        outcome({ id: "n2", state: "expired", github_issue_number: 6 }),
        outcome({ id: "s1", state: "published", github_issue_number: 7 }),
      ]),
    );
    renderView();
    await screen.findAllByTestId("factory-card");

    const expected: Record<Lane, [string, number]> = {
      queued: ["Queued", 3],
      running: ["Running", 1],
      "needs-you": ["Needs you", 2],
      shipped: ["Shipped", 1],
    };
    for (const id of LANE_IDS) {
      const [heading, count] = expected[id];
      expect(lane(id)).toHaveTextContent(heading);
      expect(within(lane(id)).getAllByText(String(count), { exact: true }).length).toBeGreaterThan(0);
      expect(cardsIn(id)).toHaveLength(count);
    }
  });

  it("omits the cancelled line when nothing is cancelled", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(list([outcome()]));
    renderView();
    await screen.findAllByTestId("factory-card");
    expect(screen.queryByTestId("factory-cancelled")).toBeNull();
  });

  it("counts several cancelled items on the cancelled line", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(
      list([
        outcome({ id: "c1", state: "cancelled", github_issue_number: 11 }),
        outcome({ id: "c2", state: "cancelled", github_issue_number: 12 }),
        outcome({ id: "c3", state: "cancelled", github_issue_number: 13 }),
        outcome({ id: "r1", state: "running", github_issue_number: 14 }),
      ]),
    );
    renderView();
    expect(await screen.findByTestId("factory-cancelled")).toHaveTextContent("3 cancelled");
    expect(screen.getAllByTestId("factory-card")).toHaveLength(1);
  });

  it("caps Shipped at 10 cards newest first and says how many more", async () => {
    // 12 published items; index 0 is the oldest, index 11 the newest. The list
    // order is scrambled so the view must sort by updated_at itself.
    const published = Array.from({ length: 12 }, (_, i) =>
      outcome({
        id: `p${i}`,
        state: "published",
        github_issue_number: 500 + i,
        title: `zq-shipped-${i}`,
        updated_at: iso((12 - i) * HOUR),
        pr: { number: 900 + i, url: `https://github.com/acme/payments/pull/${900 + i}`, status: "merged" },
      }),
    );
    const scrambled = [published[3], published[11], published[0], published[7], published[1], published[10],
      published[5], published[2], published[9], published[4], published[8], published[6]];
    vi.mocked(listWorkItems).mockResolvedValue(list(scrambled));
    renderView();

    await screen.findAllByTestId("factory-card");
    const cards = cardsIn("shipped");
    expect(cards).toHaveLength(10);
    const order = cards.map((c) => Number(/zq-shipped-(\d+)/.exec(c.textContent ?? "")?.[1]));
    expect(order).toEqual([11, 10, 9, 8, 7, 6, 5, 4, 3, 2]);
    expect(screen.queryByText("zq-shipped-0")).toBeNull();
    expect(screen.queryByText("zq-shipped-1")).toBeNull();
    expect(screen.getByTestId("factory-shipped-more")).toHaveTextContent("2 more");
  });

  it("omits the N more line when Shipped fits", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(list([outcome({ state: "published" })]));
    renderView();
    await screen.findAllByTestId("factory-card");
    expect(screen.queryByTestId("factory-shipped-more")).toBeNull();
  });

  it("computes the three KPI tiles: in flight, needs you, and shipped in the last 7 days", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(
      list([
        outcome({ id: "q1", state: "queued", github_issue_number: 1 }),
        outcome({ id: "q2", state: "waiting", github_issue_number: 2 }),
        outcome({ id: "r1", state: "running", github_issue_number: 3 }),
        outcome({ id: "r2", state: "publishing", github_issue_number: 4 }),
        outcome({ id: "r3", state: "cancellation_requested", github_issue_number: 5 }),
        outcome({ id: "n1", state: "awaiting_approval", github_issue_number: 6 }),
        outcome({ id: "n2", state: "completed_unpublished", github_issue_number: 7 }),
        outcome({ id: "s1", state: "published", github_issue_number: 8, updated_at: iso(DAY) }),
        outcome({ id: "s2", state: "published", github_issue_number: 9, updated_at: iso(6 * DAY) }),
        // Older than 7 days: in the Shipped lane but not the 7d tile.
        outcome({ id: "s3", state: "published", github_issue_number: 10, updated_at: iso(9 * DAY), title: "zq-old-ship" }),
        outcome({ id: "c1", state: "cancelled", github_issue_number: 11 }),
      ]),
    );
    renderView();
    await screen.findAllByTestId("factory-card");

    expect(screen.getByTestId("factory-kpi-in-flight")).toHaveTextContent(/\b5\b/);
    expect(screen.getByTestId("factory-kpi-needs-you")).toHaveTextContent(/\b2\b/);
    expect(screen.getByTestId("factory-kpi-shipped")).toHaveTextContent(/\b2\b/);
    expect(screen.getByTestId("factory-kpi-shipped")).not.toHaveTextContent(/\b3\b/);
    expect(cardsIn("shipped")).toHaveLength(3);
    expect(within(lane("shipped")).getByText("zq-old-ship")).toBeInTheDocument();
  });

  it("renders the lane card per Decision 7", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(
      list([
        outcome({ id: "r1", state: "running", github_issue_number: 398, progress: PROGRESS }),
        outcome({
          id: "n1",
          state: "failed",
          github_issue_number: 401,
          title: null,
          actionable_cause: "zq-needs-you-cause",
        }),
        outcome({
          id: "s1",
          state: "published",
          github_issue_number: 377,
          title: "zq-shipped-title",
          pr: { number: 4123, url: "https://github.com/acme/payments/pull/4123", status: "merged" },
        }),
      ]),
    );
    renderView();
    await screen.findAllByTestId("factory-card");

    const running = cardsIn("running")[0];
    expect(running).toHaveAttribute("role", "button");
    expect(within(running).getByRole("link", { name: /acme\/payments#398/ })).toHaveAttribute(
      "href",
      "https://github.com/acme/payments/issues/398",
    );
    expect(running).toHaveTextContent("Retry webhook delivery with exponential backoff");
    const track = within(running).getByTestId("factory-card-track");
    const segments = Array.from(track.children);
    expect(segments).toHaveLength(STAGES.length);
    expect(segments.map((s) => s.getAttribute("data-state"))).toEqual(STAGES.map((s) => s.state));

    // Null title falls back to repo#number; Needs-you cards show the cause verbatim.
    const needsYou = cardsIn("needs-you")[0];
    expect(within(needsYou).getAllByText(/acme\/payments#401/).length).toBeGreaterThanOrEqual(2);
    expect(needsYou).toHaveTextContent("zq-needs-you-cause");
    // No progress, no mini track.
    expect(within(needsYou).queryByTestId("factory-card-track")).toBeNull();

    // Shipped cards link the PR.
    const shipped = cardsIn("shipped")[0];
    expect(within(shipped).getByRole("link", { name: /^(PR )?#4123$/ })).toHaveAttribute(
      "href",
      "https://github.com/acme/payments/pull/4123",
    );

    // Only Needs-you cards show the cause.
    expect(running).not.toHaveTextContent("zq-cause-verbatim");
    expect(shipped).not.toHaveTextContent("zq-cause-verbatim");

    // Lane cards show no cost.
    for (const id of LANE_IDS) expect(lane(id).textContent).not.toContain("$");
  });

  it("draws no mini track when progress has no stages", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(
      list([outcome({ progress: { current: null, note: null, stages: [] } })]),
    );
    renderView();
    const card = (await screen.findAllByTestId("factory-card"))[0];
    expect(within(card).queryByTestId("factory-card-track")).toBeNull();
  });

  it("opens a read-only detail panel per Decision 8", async () => {
    const item = outcome({
      progress: PROGRESS,
      pr: { number: 4123, url: "https://github.com/acme/payments/pull/4123", status: "open" },
      ci: {
        state: "unavailable",
        reason: "zq-ci-reason",
        observed_at: "2026-10-06T10:06:00Z",
        head_sha: "1123456789abcdef0123456789abcdef01234567",
      },
    });
    vi.mocked(listWorkItems).mockResolvedValue(list([item]));
    vi.mocked(getWorkItem).mockResolvedValue(item);
    renderView();

    await userEvent.click((await screen.findAllByTestId("factory-card"))[0]);
    await waitFor(() => expect(getWorkItem).toHaveBeenCalledWith("wi-1"));
    await waitFor(() => expect(getWorkItemUsage).toHaveBeenCalledWith("wi-1"));

    const panel = await screen.findByTestId("factory-detail");
    expect(within(panel).getByRole("link", { name: /acme\/payments#398/ })).toHaveAttribute(
      "href",
      "https://github.com/acme/payments/issues/398",
    );
    // State pill, verbatim (the request list may also say "running").
    expect(within(panel).getAllByText("running", { exact: true }).length).toBeGreaterThan(0);
    expect(panel).toHaveTextContent("Retry webhook delivery with exponential backoff");

    const stages = within(within(panel).getByTestId("factory-stage-track")).getAllByTestId("factory-stage");
    expect(stages).toHaveLength(STAGES.length);
    STAGES.forEach((stage, i) => {
      expect(stages[i]).toHaveAttribute("data-state", stage.state);
      expect(stages[i]).toHaveTextContent(stage.label);
    });
    expect(stages[1]).toHaveTextContent("round 2 of 3");
    expect(panel).toHaveTextContent("zq-latest-note: fixing the retry jitter");

    expect(panel).toHaveTextContent("Pull request");
    expect(within(panel).getByRole("link", { name: /#4123/ })).toHaveAttribute(
      "href",
      "https://github.com/acme/payments/pull/4123",
    );
    expect(panel).toHaveTextContent("CI");
    expect(panel).toHaveTextContent("unavailable");
    expect(panel).toHaveTextContent("zq-ci-reason");
    expect(panel).toHaveTextContent("Correctness");
    expect(panel).toHaveTextContent(/owned by the bundle/i);

    const cost = await within(panel).findByTestId("factory-cost");
    expect(cost).toHaveTextContent("$1.23");
    const models = within(cost).getAllByTestId("factory-cost-model");
    expect(models).toHaveLength(2);
    expect(models[0]).toHaveTextContent("zq-model-implementer");
    expect(models[1]).toHaveTextContent("zq-model-reviewer");
    expect(cost).not.toHaveTextContent(/estimate incomplete/i);

    expect(within(panel).getByTestId("factory-requests")).toHaveTextContent("running");

    // Read-only (Decision 2): the panel has no buttons, and the view has no write verbs.
    expect(within(panel).queryAllByRole("button")).toHaveLength(0);
    expect(screen.queryByRole("button", { name: /cancel|retry|pause/i })).toBeNull();
  });

  it("keeps its detail cache apart from the Work items view", async () => {
    const item = outcome({ title: "zq-shared-title", progress: PROGRESS });
    vi.mocked(listWorkItems).mockResolvedValue(list([item]));
    vi.mocked(getWorkItem).mockResolvedValue(item);
    const Shared = wrapper();
    const factory = render(<WiredFactory />, { wrapper: Shared });
    await userEvent.click((await screen.findAllByTestId("factory-card"))[0]);
    const panel = await screen.findByTestId("factory-detail");
    await within(panel).findByText("zq-shared-title");
    factory.unmount();

    // Work items opens the same id while its own fetch is still in flight.
    // Sharing a cache entry with the Factory panel would hand it a detail of
    // the wrong shape and leave the pane blank instead of loading.
    vi.mocked(getWorkItem).mockReturnValue(new Promise<WorkItemOutcome>(() => {}));
    render(<WiredWorkItems />, { wrapper: Shared });
    await userEvent.click(await screen.findByTestId("work-item-row"));
    expect(await screen.findByText("Loading work item…")).toBeTruthy();
  });

  it("says the estimate is incomplete when cost_complete is false", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(list([outcome()]));
    vi.mocked(getWorkItem).mockResolvedValue(outcome());
    getWorkItemUsage.mockResolvedValue(usage({ cost_complete: false, requests_without_usage: 1 }));
    renderView();

    await userEvent.click((await screen.findAllByTestId("factory-card"))[0]);
    const cost = await screen.findByTestId("factory-cost");
    await waitFor(() => expect(cost).toHaveTextContent(/estimate incomplete/i));
  });

  it("says the item no longer exists when the detail is a 404", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(list([outcome()]));
    vi.mocked(getWorkItem).mockRejectedValue(new ApiError(404, "not_found"));
    getWorkItemUsage.mockRejectedValue(new ApiError(404, "not_found"));
    renderView();

    await userEvent.click((await screen.findAllByTestId("factory-card"))[0]);
    expect(await screen.findByText("This work item no longer exists.")).toBeInTheDocument();
  });

  it("renders an error notice when the list fails", async () => {
    vi.mocked(listWorkItems).mockRejectedValue(new ApiError(503, "zq-list-boom"));
    renderView();
    expect(await screen.findByText(/zq-list-boom/)).toBeInTheDocument();
  });

  it("renders no cards and no error for an empty install", async () => {
    vi.mocked(listWorkItems).mockResolvedValue(list([]));
    renderView();
    await waitFor(() => expect(listWorkItems).toHaveBeenCalled());
    await screen.findByRole("button", { name: /copy command: curie (local|cluster) work-items/i });
    expect(screen.queryAllByTestId("factory-card")).toHaveLength(0);
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("refetches the list, the open detail, and its usage every 15 seconds", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    vi.mocked(listWorkItems).mockResolvedValue(list([outcome()]));
    vi.mocked(getWorkItem).mockResolvedValue(outcome());
    renderView();

    fireEvent.click((await screen.findAllByTestId("factory-card"))[0]);
    await screen.findByTestId("factory-detail");
    await waitFor(() => expect(getWorkItemUsage).toHaveBeenCalled());

    const listCalls = vi.mocked(listWorkItems).mock.calls.length;
    const detailCalls = vi.mocked(getWorkItem).mock.calls.length;
    const usageCalls = getWorkItemUsage.mock.calls.length;

    // Not sooner than the 15 second interval.
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000);
    });
    expect(vi.mocked(listWorkItems).mock.calls.length).toBe(listCalls);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(15_000);
    });
    await waitFor(() => expect(vi.mocked(listWorkItems).mock.calls.length).toBeGreaterThan(listCalls));
    await waitFor(() => expect(vi.mocked(getWorkItem).mock.calls.length).toBeGreaterThan(detailCalls));
    await waitFor(() => expect(getWorkItemUsage.mock.calls.length).toBeGreaterThan(usageCalls));
  });
});
