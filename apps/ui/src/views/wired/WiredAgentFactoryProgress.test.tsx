import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useEffect } from "react";
import { WiredAgentFactoryProgress } from "./WiredAgentFactoryProgress";
import { StoreProvider, useStore } from "../../state/store";
import type { AgentOut } from "../../api/client";

const UNBOUND_BODY = { detail: { code: "factory_slack_channel_unbound" } };
const PATTERN_BODY = {
  detail: [
    {
      loc: ["body", "factory_slack_channel"],
      msg: "String should match pattern '^[CG][A-Z0-9]{6,}$'",
      type: "string_pattern_mismatch",
    },
  ],
};

const AGENT = { id: "a1", name: "deal-desk" } as unknown as AgentOut;

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

// Reads the store so a test can see the toast and pick the console environment.
function StoreProbe({ env }: { env: "prod" | "dev" }) {
  const { state, dispatch } = useStore();
  useEffect(() => {
    dispatch({ type: "setEnv", env });
  }, [dispatch, env]);
  return <div data-testid="toast-probe">{state.toast ?? ""}</div>;
}

function Harness({
  channel,
  onSaved,
  env = "prod",
}: {
  channel: string | null;
  onSaved: () => void;
  env?: "prod" | "dev";
}) {
  return (
    <StoreProvider>
      <StoreProbe env={env} />
      <WiredAgentFactoryProgress agentId="a1" agentName="deal-desk" channel={channel} onSaved={onSaved} />
    </StoreProvider>
  );
}

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn();
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

// The one PATCH the component sent: its URL, method and parsed JSON body.
function sentPatch(callIndex = 0): { url: string; method: string; body: unknown } {
  const [url, init] = fetchMock.mock.calls[callIndex] as [string, RequestInit];
  return { url, method: String(init.method), body: JSON.parse(String(init.body)) };
}

describe("WiredAgentFactoryProgress", () => {
  it("renders the current channel seeded into the input", () => {
    render(<Harness channel="C0EXAMPLE1" onSaved={vi.fn()} />);
    expect(screen.getByLabelText("progress channel")).toHaveValue("C0EXAMPLE1");
    expect(screen.getByTestId("factory-progress-current")).toHaveTextContent("Posting progress to C0EXAMPLE1");
    expect(screen.getByTestId("factory-progress-clear")).toBeEnabled();
  });

  it("renders the empty state with Clear disabled when no channel is set", () => {
    render(<Harness channel={null} onSaved={vi.fn()} />);
    const input = screen.getByTestId("factory-progress-input");
    expect(input).toHaveValue("");
    expect(input).toHaveAttribute("placeholder", "none");
    expect(screen.getByTestId("factory-progress-current")).toHaveTextContent("No Slack progress channel set");
    expect(screen.getByTestId("factory-progress-clear")).toBeDisabled();
  });

  it("reseeds the input when the stored channel or the agent changes", () => {
    const { rerender } = render(<Harness channel="C0EXAMPLE1" onSaved={vi.fn()} />);
    rerender(<Harness channel="C0EXAMPLE2" onSaved={vi.fn()} />);
    expect(screen.getByTestId("factory-progress-input")).toHaveValue("C0EXAMPLE2");
    rerender(<Harness channel={null} onSaved={vi.fn()} />);
    expect(screen.getByTestId("factory-progress-input")).toHaveValue("");
  });

  it("sends exactly the channel on save, refetches, and toasts", async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    fetchMock.mockResolvedValue(jsonResponse(200, { ...AGENT, factory_slack_channel: "C0EXAMPLE1" }));
    render(<Harness channel={null} onSaved={onSaved} />);

    await user.type(screen.getByTestId("factory-progress-input"), "  C0EXAMPLE1  ");
    await user.click(screen.getByTestId("factory-progress-save"));

    await waitFor(() => expect(onSaved).toHaveBeenCalledTimes(1));
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(sentPatch()).toEqual({
      url: "/api/agents/a1",
      method: "PATCH",
      body: { factory_slack_channel: "C0EXAMPLE1" },
    });
    expect(screen.getByTestId("toast-probe")).toHaveTextContent("Progress channel set to C0EXAMPLE1");
  });

  it("sends an explicit null on clear, refetches, and toasts", async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    fetchMock.mockResolvedValue(jsonResponse(200, { ...AGENT, factory_slack_channel: null }));
    render(<Harness channel="C0EXAMPLE1" onSaved={onSaved} />);

    await user.click(screen.getByTestId("factory-progress-clear"));

    await waitFor(() => expect(onSaved).toHaveBeenCalledTimes(1));
    expect(sentPatch().body).toEqual({ factory_slack_channel: null });
    expect(Object.keys(sentPatch().body as object)).toEqual(["factory_slack_channel"]);
    expect(screen.getByTestId("toast-probe")).toHaveTextContent("Progress channel cleared");
  });

  it("explains an unbound 409 and shows the cluster surfaces command in prod", async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    fetchMock.mockResolvedValue(jsonResponse(409, UNBOUND_BODY));
    render(<Harness channel={null} onSaved={onSaved} env="prod" />);

    await user.type(screen.getByTestId("factory-progress-input"), "C0EXAMPLE1");
    await user.click(screen.getByTestId("factory-progress-save"));

    const error = await screen.findByTestId("factory-progress-error");
    expect(error).toHaveTextContent(
      "This agent has no Slack binding for C0EXAMPLE1. Add the channel as a surface first, then set it here.",
    );
    expect(error).not.toHaveTextContent("Conflict");
    expect(
      screen.getByRole("button", { name: "Copy command: curie cluster surfaces deal-desk --add slack=C0EXAMPLE1" }),
    ).toBeInTheDocument();
    expect(onSaved).not.toHaveBeenCalled();
  });

  it("shows the local surfaces command in dev", async () => {
    const user = userEvent.setup();
    fetchMock.mockResolvedValue(jsonResponse(409, UNBOUND_BODY));
    render(<Harness channel={null} onSaved={vi.fn()} env="dev" />);

    await user.type(screen.getByTestId("factory-progress-input"), "C0EXAMPLE1");
    await user.click(screen.getByTestId("factory-progress-save"));

    await screen.findByTestId("factory-progress-error");
    expect(
      screen.getByRole("button", { name: "Copy command: curie local surfaces deal-desk --add slack=C0EXAMPLE1" }),
    ).toBeInTheDocument();
  });

  it("renders the format message for a server 422 pattern failure", async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    fetchMock.mockResolvedValue(jsonResponse(422, PATTERN_BODY));
    render(<Harness channel={null} onSaved={onSaved} />);

    // Passes the client check, so the request goes out and the server refuses it.
    await user.type(screen.getByTestId("factory-progress-input"), "C0EXAMPLE1");
    await user.click(screen.getByTestId("factory-progress-save"));

    const error = await screen.findByTestId("factory-progress-error");
    expect(error).toHaveTextContent("Not a Slack channel id: use a public or private channel id such as C0EXAMPLE1");
    expect(error).not.toHaveTextContent("String should match pattern");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(onSaved).not.toHaveBeenCalled();
  });

  it("rejects a malformed id client-side without calling the API", async () => {
    const user = userEvent.setup();
    render(<Harness channel={null} onSaved={vi.fn()} />);

    await user.type(screen.getByTestId("factory-progress-input"), "general");
    await user.click(screen.getByTestId("factory-progress-save"));

    expect(screen.getByTestId("factory-progress-error")).toHaveTextContent(
      "Not a Slack channel id: use a public or private channel id such as C0EXAMPLE1 " +
        "(uppercase letters and digits, at least 7 characters, starting with C or G).",
    );
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("does not treat a blank box as a clear and makes no request", async () => {
    const user = userEvent.setup();
    render(<Harness channel="C0EXAMPLE1" onSaved={vi.fn()} />);

    await user.clear(screen.getByTestId("factory-progress-input"));
    await user.click(screen.getByTestId("factory-progress-save"));

    expect(screen.getByTestId("factory-progress-error")).toHaveTextContent("use Clear");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("renders any other failure with its message", async () => {
    const user = userEvent.setup();
    fetchMock.mockResolvedValue(jsonResponse(500, { detail: "database offline" }));
    render(<Harness channel={null} onSaved={vi.fn()} />);

    await user.type(screen.getByTestId("factory-progress-input"), "C0EXAMPLE1");
    await user.click(screen.getByTestId("factory-progress-save"));

    expect(await screen.findByTestId("factory-progress-error")).toHaveTextContent(
      "Could not update progress channel: database offline",
    );
  });

  it("clears a stale error once a later save succeeds", async () => {
    const user = userEvent.setup();
    const onSaved = vi.fn();
    fetchMock
      .mockResolvedValueOnce(jsonResponse(409, UNBOUND_BODY))
      .mockResolvedValueOnce(jsonResponse(200, { ...AGENT, factory_slack_channel: "C0EXAMPLE2" }));
    render(<Harness channel={null} onSaved={onSaved} />);

    const input = screen.getByTestId("factory-progress-input");
    await user.type(input, "C0EXAMPLE1");
    await user.click(screen.getByTestId("factory-progress-save"));
    await screen.findByTestId("factory-progress-error");

    await user.clear(input);
    await user.type(input, "C0EXAMPLE2");
    await user.click(screen.getByTestId("factory-progress-save"));

    await waitFor(() => expect(onSaved).toHaveBeenCalledTimes(1));
    expect(screen.queryByTestId("factory-progress-error")).toBeNull();
    expect(sentPatch(1).body).toEqual({ factory_slack_channel: "C0EXAMPLE2" });
  });

  it("offers the set command for the console environment", () => {
    const { unmount } = render(<Harness channel={null} onSaved={vi.fn()} env="prod" />);
    expect(
      screen.getByRole("button", { name: "Copy command: curie cluster factory-progress deal-desk" }),
    ).toBeInTheDocument();
    unmount();

    render(<Harness channel={null} onSaved={vi.fn()} env="dev" />);
    expect(
      screen.getByRole("button", { name: "Copy command: curie local factory-progress deal-desk" }),
    ).toBeInTheDocument();
  });

  it("includes the typed channel in the set command once it is valid", async () => {
    const user = userEvent.setup();
    render(<Harness channel={null} onSaved={vi.fn()} env="prod" />);
    await user.type(screen.getByTestId("factory-progress-input"), "C0EXAMPLE9");
    expect(
      screen.getByRole("button", {
        name: "Copy command: curie cluster factory-progress deal-desk --slack-channel C0EXAMPLE9",
      }),
    ).toBeInTheDocument();
  });
});
