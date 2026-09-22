import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StoreProvider } from "../../state/store";
import { RealApprovals } from "./RealApprovals";
import {
  ApiError,
  exchangeConsoleLoginCode,
  getApprovalAudit,
  getConsoleSession,
  listApprovals,
  resolveApproval,
  type ApprovalAudit,
  type ApprovalOut,
} from "../../api/client";

// Mock only the approvals data-layer calls; keep everything else (ApiError, the
// store, primitives) real.
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    listApprovals: vi.fn(),
    getApprovalAudit: vi.fn(),
    getConsoleSession: vi.fn(),
    exchangeConsoleLoginCode: vi.fn(),
    resolveApproval: vi.fn(),
  };
});

function consoleSession(subject = "U0AUTHENTICATED") {
  return {
    subject,
    expires_at: "2026-07-24T12:00:00+00:00",
  };
}

const RESOLVE_ERROR_CASES: Array<[number, string, string]> = [
  [403, "not an eligible approver", "Not authorized: not an eligible approver"],
  [409, "already resolved by U-bob (approved)", "Already resolved: already resolved by U-bob"],
  [410, "approval expired", "Expired: approval expired"],
];

function approval(overrides: Partial<ApprovalOut> = {}): ApprovalOut {
  return {
    id: "ap-1",
    agent_id: "ag-1",
    conversation_id: "C-thread-1",
    author: "U-alice",
    summary: "Refund $4,200 to ACME Corp",
    reply_channel: "C0DEALS",
    reply_placeholder: "ts-1",
    reply_endpoint: null,
    dedupe_key: "dk-1",
    route: "managers",
    card_channel: "C0MANAGERS",
    gate_kind: "permission",
    granted_tool: "issue_refund",
    status: "pending",
    expires_at: "2026-07-24T00:00:00+00:00",
    resolved_by: null,
    resolution_note: null,
    created_at: "2026-07-23T00:00:00+00:00",
    resolved_at: null,
    ...overrides,
  };
}

function renderView() {
  return render(
    <StoreProvider>
      <RealApprovals />
    </StoreProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
  vi.mocked(getApprovalAudit).mockResolvedValue([]);
  vi.mocked(getConsoleSession).mockResolvedValue(consoleSession());
  try {
    window.localStorage.clear();
  } catch {
    // ignore
  }
});

describe("RealApprovals (#867)", () => {
  it("lists pending approvals by default and requests the pending status", async () => {
    vi.mocked(listApprovals).mockResolvedValue([approval()]);
    renderView();

    expect(await screen.findByText("Refund $4,200 to ACME Corp")).toBeInTheDocument();
    expect(screen.getByText("U-alice")).toBeInTheDocument();
    expect(listApprovals).toHaveBeenCalledWith({ status: "pending" });
  });

  it("shows the pending empty state when nothing is waiting", async () => {
    vi.mocked(listApprovals).mockResolvedValue([]);
    renderView();
    expect(await screen.findByText(/No pending approvals/i)).toBeInTheDocument();
  });

  it("refetches with the chosen status filter (all sends no status)", async () => {
    vi.mocked(listApprovals).mockResolvedValue([]);
    renderView();
    await screen.findByText(/No pending approvals/i);

    await userEvent.selectOptions(screen.getByTestId("approvals-status-filter"), "all");
    await waitFor(() => expect(listApprovals).toHaveBeenLastCalledWith({ status: undefined }));
  });

  it("surfaces a load error", async () => {
    vi.mocked(listApprovals).mockRejectedValue(new Error("boom"));
    renderView();
    expect(await screen.findByTestId("approvals-error")).toHaveTextContent("boom");
  });

  it("opens a detail modal with an audit trail that distinguishes authenticated and historical entries", async () => {
    vi.mocked(listApprovals).mockResolvedValue([approval()]);
    const audit: ApprovalAudit[] = [
      {
        id: "au-authenticated",
        approval_id: "ap-1",
        action: "resolved",
        actor: "U0AUTHENTICATED",
        actor_channel: null,
        principal_kind: "console",
        principal_subject: null,
        authenticated: true,
        decision: "approved",
        authorizer: "explicit-users",
        authorized: true,
        reason: null,
        evidence: null,
        created_at: "2026-07-23T01:00:00+00:00",
      },
      {
        id: "au-historical",
        approval_id: "ap-1",
        action: "denied",
        actor: "U-legacy",
        actor_channel: "C0DEALS",
        principal_kind: null,
        principal_subject: null,
        authenticated: false,
        decision: "approved",
        authorizer: "legacy-authorizer",
        authorized: false,
        reason: "recorded before authenticated principals",
        evidence: null,
        created_at: "2026-07-23T01:01:00+00:00",
      },
    ];
    vi.mocked(getApprovalAudit).mockResolvedValue(audit);
    renderView();

    await userEvent.click(await screen.findByText("Refund $4,200 to ACME Corp"));
    const detail = await screen.findByTestId("approval-detail");
    expect(within(detail).getByText("managers")).toBeInTheDocument();
    const entries = await within(detail).findAllByTestId("approval-audit-entry");
    expect(entries[0]).toHaveTextContent("principal: console (authenticated)");
    expect(entries[1]).toHaveTextContent("principal: historical (not authenticated)");
  });

  it("uses the immutable authenticated session subject, not free-text or localStorage identity, when resolving", async () => {
    vi.mocked(listApprovals).mockResolvedValue([approval()]);
    vi.mocked(resolveApproval).mockResolvedValue(approval({ status: "approved", resolved_by: "U0AUTHENTICATED" }));
    window.localStorage.setItem("curie.approvalOperator", "U0FORGED");
    renderView();

    await userEvent.click(await screen.findByText("Refund $4,200 to ACME Corp"));
    const detail = await screen.findByTestId("approval-detail");
    expect(within(detail).getByTestId("approval-principal")).toHaveTextContent("U0AUTHENTICATED");
    expect(within(detail).queryByLabelText("resolved by")).not.toBeInTheDocument();
    expect(within(detail).queryByLabelText("actor channel")).not.toBeInTheDocument();
    expect(within(detail).queryByText("U0FORGED")).not.toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("note"), "Confirmed from the console");
    await userEvent.click(screen.getByTestId("approve-btn"));

    await waitFor(() =>
      expect(resolveApproval).toHaveBeenCalledWith("ap-1", {
        decision: "approved",
        note: "Confirmed from the console",
      }),
    );
    // Refetch fired after resolve (initial load + reload).
    await waitFor(() => expect(listApprovals).toHaveBeenCalledTimes(2));
  });

  it("shows the login-code exchange when there is no console session and renders its immutable subject after exchange", async () => {
    vi.mocked(listApprovals).mockResolvedValue([approval()]);
    vi.mocked(getConsoleSession).mockRejectedValue(new ApiError(401, "missing, invalid, or expired console session"));
    vi.mocked(exchangeConsoleLoginCode).mockResolvedValue(consoleSession("U0EXCHANGED"));
    renderView();

    expect(await screen.findByLabelText("login code")).toBeInTheDocument();
    expect(screen.queryByTestId("approve-btn")).not.toBeInTheDocument();
    await userEvent.type(screen.getByLabelText("login code"), "one-time-example-code");
    await userEvent.click(screen.getByTestId("approval-login-submit"));

    await waitFor(() => expect(exchangeConsoleLoginCode).toHaveBeenCalledWith("one-time-example-code"));
    expect(await screen.findByTestId("approval-principal")).toHaveTextContent("U0EXCHANGED");
    expect(screen.queryByLabelText("resolved by")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("actor channel")).not.toBeInTheDocument();
  });

  it("returns to the login-code state when an otherwise-live session is revoked or expires", async () => {
    vi.mocked(listApprovals).mockResolvedValue([approval()]);
    vi.mocked(resolveApproval).mockRejectedValue(new ApiError(401, "missing, invalid, or expired console session"));
    renderView();

    await userEvent.click(await screen.findByText("Refund $4,200 to ACME Corp"));
    await screen.findByTestId("approval-detail");
    await userEvent.click(screen.getByTestId("reject-btn"));

    expect(await screen.findByLabelText("login code")).toBeInTheDocument();
    expect(screen.queryByTestId("approve-btn")).not.toBeInTheDocument();
  });

  it.each(RESOLVE_ERROR_CASES)("preserves the designed resolve message for HTTP %i", async (status, detail, message) => {
    vi.mocked(listApprovals).mockResolvedValue([approval()]);
    vi.mocked(resolveApproval).mockRejectedValue(new ApiError(status, detail));
    renderView();

    await userEvent.click(await screen.findByText("Refund $4,200 to ACME Corp"));
    await screen.findByTestId("approval-detail");
    await userEvent.click(screen.getByTestId("reject-btn"));

    expect(await screen.findByTestId("resolve-error")).toHaveTextContent(message);
  });

  it("hides the resolve controls for an already-resolved approval", async () => {
    vi.mocked(listApprovals).mockResolvedValue([
      approval({ status: "approved", resolved_by: "U-bob", resolution_note: "ok" }),
    ]);
    renderView();

    await userEvent.click(await screen.findByText("Refund $4,200 to ACME Corp"));
    await screen.findByTestId("approval-detail");
    expect(screen.queryByTestId("approve-btn")).not.toBeInTheDocument();
  });
});
