import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StoreProvider } from "../../state/store";
import { WiredAgentState } from "./WiredAgentState";
import {
  ApiError,
  listStateNamespaces,
  listStateEntries,
  type StateNamespace,
  type StateEntry,
} from "../../api/client";

// Mock only the state data-layer calls; keep everything else real.
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    listStateNamespaces: vi.fn(),
    listStateEntries: vi.fn(),
  };
});

const NAMESPACES: StateNamespace[] = [
  { namespace: "approvals", key_count: 2, last_updated: "2026-07-20T10:00:00+00:00" },
  { namespace: "dedupe", key_count: 1, last_updated: "2026-07-19T10:00:00+00:00" },
];

const ENTRIES: StateEntry[] = [
  { namespace: "approvals", key: "older", value: { n: 1 }, version: 1, updated_at: "2026-07-20T09:00:00+00:00" },
  { namespace: "approvals", key: "newer", value: { ok: true }, version: 3, updated_at: "2026-07-20T10:00:00+00:00" },
];

function renderPanel() {
  return render(
    <StoreProvider>
      <WiredAgentState agentId="a1" />
    </StoreProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("WiredAgentState (#250)", () => {
  it("lists the agent's namespaces with key counts", async () => {
    vi.mocked(listStateNamespaces).mockResolvedValue(NAMESPACES);
    renderPanel();

    expect(await screen.findByText("approvals")).toBeInTheDocument();
    expect(screen.getByText("dedupe")).toBeInTheDocument();
    expect(screen.getByText("2 keys")).toBeInTheDocument();
    expect(screen.getByText("1 key")).toBeInTheDocument();
  });

  it("shows the empty state when nothing has been stored", async () => {
    vi.mocked(listStateNamespaces).mockResolvedValue([]);
    renderPanel();
    expect(await screen.findByText(/has not stored any durable state yet/i)).toBeInTheDocument();
  });

  it("lists a namespace's keys newest-first with value and version on selection", async () => {
    vi.mocked(listStateNamespaces).mockResolvedValue(NAMESPACES);
    vi.mocked(listStateEntries).mockResolvedValue(ENTRIES);
    renderPanel();

    await userEvent.click(await screen.findByText("approvals"));
    await waitFor(() => expect(listStateEntries).toHaveBeenCalledWith("a1", "approvals"));

    const rendered = await screen.findAllByTestId("state-entry");
    // Re-sorted by updated_at desc: "newer" (10:00) before "older" (09:00).
    expect(rendered[0]).toHaveTextContent("newer");
    expect(rendered[1]).toHaveTextContent("older");
    expect(screen.getByText("v3")).toBeInTheDocument();
    expect(screen.getByText(/"ok": true/)).toBeInTheDocument();
  });

  it("surfaces a load error", async () => {
    vi.mocked(listStateNamespaces).mockRejectedValue(new Error("boom"));
    renderPanel();
    expect(await screen.findByTestId("state-error")).toHaveTextContent("boom");
    // A failed load is not evidence that nothing was stored.
    expect(screen.queryByText(/has not stored any durable state/i)).not.toBeInTheDocument();
  });
});

// #1047: the state routes accept only the platform key or a sandbox state
// token, never a console session (apps/api/src/curie_api/routers/state.py
// require_state_access: 401 "missing or invalid credential"; pinned by
// apps/api/tests/test_console_session_api_auth.py). Under cookie auth the card
// says so honestly instead of claiming the agent stored nothing.
const STATE_401 = "missing or invalid credential";

describe("WiredAgentState under console-session auth (#1047)", () => {
  it("shows the unavailable notice, not an error or the empty state, when the state read is refused", async () => {
    vi.mocked(listStateNamespaces).mockRejectedValue(new ApiError(401, STATE_401));
    renderPanel();

    expect(await screen.findByTestId("state-unavailable")).toHaveTextContent(/console session/i);
    expect(screen.queryByTestId("state-error")).not.toBeInTheDocument();
    expect(screen.queryByText(/has not stored any durable state/i)).not.toBeInTheDocument();
  });

  it("shows a server failure as an error, without the unavailable notice or the empty state", async () => {
    vi.mocked(listStateNamespaces).mockRejectedValue(new ApiError(500, "Internal Server Error"));
    renderPanel();

    expect(await screen.findByTestId("state-error")).toHaveTextContent("Internal Server Error");
    expect(screen.queryByTestId("state-unavailable")).not.toBeInTheDocument();
    expect(screen.queryByText(/has not stored any durable state/i)).not.toBeInTheDocument();
  });

  it("clears the refusal when a refresh succeeds", async () => {
    vi.mocked(listStateNamespaces).mockRejectedValueOnce(new ApiError(401, STATE_401)).mockResolvedValue(NAMESPACES);
    renderPanel();
    await screen.findByTestId("state-unavailable");

    await userEvent.click(screen.getByRole("button", { name: "Refresh" }));

    expect(await screen.findByText("approvals")).toBeInTheDocument();
    expect(screen.queryByTestId("state-unavailable")).not.toBeInTheDocument();
  });

  it("shows the unavailable notice in the entries pane when a namespace read is refused", async () => {
    vi.mocked(listStateNamespaces).mockResolvedValue(NAMESPACES);
    vi.mocked(listStateEntries).mockRejectedValue(new ApiError(401, STATE_401));
    renderPanel();

    await userEvent.click(await screen.findByText("approvals"));

    expect(await screen.findByTestId("state-unavailable")).toHaveTextContent(/console session/i);
    // The namespace list stays; only the entries pane reports the refusal.
    expect(screen.getByText("dedupe")).toBeInTheDocument();
    expect(screen.queryByText(STATE_401)).not.toBeInTheDocument();
    expect(screen.queryAllByTestId("state-entry")).toHaveLength(0);
  });
});
