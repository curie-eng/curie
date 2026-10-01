import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StoreProvider } from "../../state/store";
import { WiredThreadReset } from "./WiredThreadReset";
import { resetThread, getThreadResetState, ApiError } from "../../api/client";

// Mock only the thread-reset data-layer calls; keep everything else real.
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    resetThread: vi.fn(),
    getThreadResetState: vi.fn(),
  };
});

function renderPanel() {
  return render(
    <StoreProvider>
      <WiredThreadReset agentId="a1" agentName="deal-desk" />
    </StoreProvider>,
  );
}

beforeEach(() => {
  vi.clearAllMocks();
});

describe("WiredThreadReset (#871)", () => {
  it("gates the destructive reset behind a confirm step", async () => {
    renderPanel();
    // The start button is disabled until a thread key is entered.
    expect(screen.getByTestId("thread-reset-start")).toBeDisabled();

    await userEvent.type(screen.getByTestId("thread-reset-key"), "1699.0012");
    await userEvent.click(screen.getByTestId("thread-reset-start"));

    // Confirm affordance appears; no request has fired yet.
    expect(screen.getByTestId("thread-reset-confirm")).toBeInTheDocument();
    expect(resetThread).not.toHaveBeenCalled();

    // Cancel backs out without firing.
    await userEvent.click(screen.getByTestId("thread-reset-cancel"));
    expect(screen.getByTestId("thread-reset-start")).toBeInTheDocument();
    expect(resetThread).not.toHaveBeenCalled();
  });

  it("requests the reset and polls to a released outcome", async () => {
    vi.mocked(resetThread).mockResolvedValue({ requested: true });
    // First poll already reads released (requested false).
    vi.mocked(getThreadResetState).mockResolvedValue({ requested: false });

    renderPanel();
    await userEvent.type(screen.getByTestId("thread-reset-key"), "1699.0012");
    await userEvent.click(screen.getByTestId("thread-reset-start"));
    await userEvent.click(screen.getByTestId("thread-reset-confirm"));

    expect(resetThread).toHaveBeenCalledWith("a1", "1699.0012");
    const released = await screen.findByTestId("thread-reset-released");
    expect(released).toHaveTextContent("1699.0012");
    expect(getThreadResetState).toHaveBeenCalledWith("a1", "1699.0012");
  });

  it("reports that no route matched when the drained reset released nothing", async () => {
    // #3699: the worker drained the reset but the key matched no route, so no
    // sandbox was released. That must not read as a successful release.
    vi.mocked(resetThread).mockResolvedValue({ requested: true, route_existed: null });
    vi.mocked(getThreadResetState).mockResolvedValue({ requested: false, route_existed: false });

    renderPanel();
    await userEvent.type(screen.getByTestId("thread-reset-key"), "slack:C0EXAMPLE1:missing");
    await userEvent.click(screen.getByTestId("thread-reset-start"));
    await userEvent.click(screen.getByTestId("thread-reset-confirm"));

    const noRoute = await screen.findByTestId("thread-reset-no-route");
    expect(noRoute).toHaveTextContent(
      "no route matched thread slack:C0EXAMPLE1:missing; nothing was released",
    );
    expect(screen.queryByTestId("thread-reset-released")).not.toBeInTheDocument();
  });

  it("still reports a release when the API names the route that was released", async () => {
    vi.mocked(resetThread).mockResolvedValue({ requested: true, route_existed: null });
    vi.mocked(getThreadResetState).mockResolvedValue({ requested: false, route_existed: true });

    renderPanel();
    await userEvent.type(screen.getByTestId("thread-reset-key"), "1699.0012");
    await userEvent.click(screen.getByTestId("thread-reset-start"));
    await userEvent.click(screen.getByTestId("thread-reset-confirm"));

    expect(await screen.findByTestId("thread-reset-released")).toHaveTextContent("1699.0012");
    expect(screen.queryByTestId("thread-reset-no-route")).not.toBeInTheDocument();
  });

  it("reports the release as still pending when polling cannot confirm it", async () => {
    vi.mocked(resetThread).mockResolvedValue({ requested: true });
    // A poll failure degrades to "still pending", never fails the accepted reset.
    vi.mocked(getThreadResetState).mockRejectedValue(new Error("gone"));

    renderPanel();
    await userEvent.type(screen.getByTestId("thread-reset-key"), "1699.0012");
    await userEvent.click(screen.getByTestId("thread-reset-start"));
    await userEvent.click(screen.getByTestId("thread-reset-confirm"));

    expect(await screen.findByTestId("thread-reset-pending")).toBeInTheDocument();
  });

  it("surfaces a request error without leaving the confirm state stuck", async () => {
    vi.mocked(resetThread).mockRejectedValue(new ApiError(404, "agent not found"));

    renderPanel();
    await userEvent.type(screen.getByTestId("thread-reset-key"), "nope");
    await userEvent.click(screen.getByTestId("thread-reset-start"));
    await userEvent.click(screen.getByTestId("thread-reset-confirm"));

    expect(await screen.findByTestId("thread-reset-error")).toHaveTextContent("agent not found");
    // Back to the entry state, not stuck mid-request.
    expect(screen.getByTestId("thread-reset-start")).toBeInTheDocument();
    expect(getThreadResetState).not.toHaveBeenCalled();
  });

  it("offers a copyable env-scoped reset-thread CLI command", async () => {
    renderPanel();
    await userEvent.type(screen.getByTestId("thread-reset-key"), "1699.0012");
    // Default env is prod -> cluster reset-thread, carrying the entered key and --yes.
    expect(
      screen.getByRole("button", {
        name: "Copy command: curie cluster reset-thread deal-desk --thread-key 1699.0012 --yes",
      }),
    ).toBeInTheDocument();
  });
});
