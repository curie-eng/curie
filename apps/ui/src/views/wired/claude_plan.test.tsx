import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { StoreProvider } from "../../state/store";
import { ConsoleSessionGate } from "../../state/session";
import { WiredSettings } from "./WiredStubs";
import { WiredProvider } from "../../state/wired";

// The real view, client and session gate run over a stubbed fetch. Bodies mirror
// apps/api/src/curie_api/schemas/personal_enrollments.py, and a refusal is
// {"detail": {"code": ...}} as personal_enrollments.py raises it.

const EXPIRES_AT = "2026-07-24T12:00:00+00:00";
const PRINCIPAL = {
  subject: "principal:p1",
  expires_at: EXPIRES_AT,
  kind: "principal",
  principal: { id: "p1", display_name: "Alice Example", email: "alice@example.com" },
};
const OPERATOR = { subject: "operator@example.com", expires_at: EXPIRES_AT, kind: "operator", principal: null };
const SENTINEL = "SENTINEL-token-0123456789-never-echo";

type Enrollment = {
  state: "absent" | "active" | "inactive";
  generation: number | null;
  version: number | null;
  teardown: { pending_runtimes: number; pending_custody_cleanup: number };
};

const absent = (): Enrollment => ({
  state: "absent",
  generation: null,
  version: null,
  teardown: { pending_runtimes: 0, pending_custody_cleanup: 0 },
});
const active = (generation = 1, version = 3): Enrollment => ({
  state: "active",
  generation,
  version,
  teardown: { pending_runtimes: 0, pending_custody_cleanup: 0 },
});
const inactive = (version = 4, runs = 0, tokens = 0): Enrollment => ({
  state: "inactive",
  generation: 2,
  version,
  teardown: { pending_runtimes: runs, pending_custody_cleanup: tokens },
});

function json(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}
const refusal = (status: number, code: string) => json(status, { detail: { code } });

type Handler = (init: RequestInit) => Response | Promise<Response>;
let routes: Record<string, Handler>;
let fetchMock: ReturnType<typeof vi.fn>;
let queryClient: QueryClient;

function calls(method: string, path: string): RequestInit[] {
  return fetchMock.mock.calls
    .filter(([u, init]) => u === path && ((init as RequestInit | undefined)?.method ?? "GET") === method)
    .map(([, init]) => (init ?? {}) as RequestInit);
}

beforeEach(() => {
  routes = { "GET /api/console/session": () => json(200, PRINCIPAL) };
  fetchMock = vi.fn((input: string, init: RequestInit = {}) => {
    const key = `${(init.method ?? "GET").toUpperCase()} ${input.split("?")[0]}`;
    const handler = routes[key];
    return Promise.resolve(handler ? handler(init) : json(404, { detail: "Not Found" }));
  });
  vi.stubGlobal("fetch", fetchMock);
  routes["GET /api/agents"] = () => json(200, []);
  routes["GET /api/config"] = () => json(200, { org_name: "Acme" });
  window.history.pushState({}, "", "/settings/claude-plan");
  localStorage.clear();
  sessionStorage.clear();
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.useRealTimers();
  window.history.pushState({}, "", "/");
});

function setEnrollment(next: () => Enrollment) {
  routes["GET /api/personal-claude/enrollment"] = () => json(200, next());
}

function mount() {
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } } });
  return render(
    <QueryClientProvider client={queryClient}>
      <StoreProvider>
        <ConsoleSessionGate>
          <WiredProvider>
            <WiredSettings />
          </WiredProvider>
        </ConsoleSessionGate>
      </StoreProvider>
    </QueryClientProvider>,
  );
}

const status = () => screen.findByTestId("claude-plan-status");
const tokenBox = () => screen.getByTestId("claude-plan-token") as HTMLInputElement;

describe("claude_plan states", () => {
  it("claude_plan absent shows Not connected, a paste box and Connect only", async () => {
    setEnrollment(absent);
    mount();
    expect(await status()).toHaveTextContent("Not connected");
    expect(screen.getByTestId("claude-plan-write")).toHaveTextContent("Connect");
    expect(screen.queryByTestId("claude-plan-disconnect")).toBeNull();
    expect(screen.queryByTestId("claude-plan-teardown")).toBeNull();
    expect(tokenBox()).toHaveAttribute("type", "password");
    expect(tokenBox()).toHaveAttribute("autocomplete", "off");
    expect(tokenBox()).toHaveAttribute("autocorrect", "off");
    expect(tokenBox()).toHaveAttribute("spellcheck", "false");
    expect(screen.getByText(/never shows it again/)).toBeInTheDocument();
  });

  it("claude_plan active shows the generation, Replace and Disconnect", async () => {
    setEnrollment(() => active(2, 5));
    mount();
    expect(await status()).toHaveTextContent("Connected, generation 2");
    expect(screen.getByTestId("claude-plan-write")).toHaveTextContent("Replace");
    expect(screen.getByTestId("claude-plan-disconnect")).toBeInTheDocument();
    expect(screen.getByText(/does not revoke the token with Anthropic/)).toBeInTheDocument();
  });

  it("claude_plan inactive shows Disconnected and Reconnect only", async () => {
    setEnrollment(() => inactive());
    mount();
    expect(await status()).toHaveTextContent("Disconnected");
    expect(screen.getByTestId("claude-plan-write")).toHaveTextContent("Reconnect");
    expect(screen.queryByTestId("claude-plan-disconnect")).toBeNull();
  });

  it("claude_plan teardown line shows both counts and polls every 5 seconds until zero", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    const reads = [inactive(4, 2, 1), inactive(4, 0, 1), inactive(4, 0, 0)];
    let n = 0;
    routes["GET /api/personal-claude/enrollment"] = () => json(200, reads[Math.min(n++, reads.length - 1)]);
    mount();
    expect(await screen.findByTestId("claude-plan-teardown")).toHaveTextContent(
      "Disconnect still finishing: 2 runs and 1 stored tokens to clean up.",
    );
    await act(() => vi.advanceTimersByTimeAsync(5100));
    await waitFor(() =>
      expect(screen.getByTestId("claude-plan-teardown")).toHaveTextContent("0 runs and 1 stored tokens"),
    );
    await act(() => vi.advanceTimersByTimeAsync(5100));
    await waitFor(() => expect(screen.queryByTestId("claude-plan-teardown")).toBeNull());
    const settled = calls("GET", "/api/personal-claude/enrollment").length;
    await act(() => vi.advanceTimersByTimeAsync(30000));
    expect(calls("GET", "/api/personal-claude/enrollment").length).toBe(settled);
  });
});

describe("claude_plan actions", () => {
  async function paste(value: string) {
    await userEvent.type(tokenBox(), value);
  }

  it("claude_plan Connect sends enroll with a null version", async () => {
    let current = absent();
    setEnrollment(() => current);
    routes["PUT /api/personal-claude/enrollment"] = () => {
      current = active(1, 1);
      return json(200, current);
    };
    mount();
    await status();
    await paste(SENTINEL);
    await userEvent.click(screen.getByTestId("claude-plan-write"));
    await waitFor(() => expect(screen.getByTestId("claude-plan-status")).toHaveTextContent("Connected, generation 1"));
    const [put] = calls("PUT", "/api/personal-claude/enrollment");
    expect(JSON.parse(put.body as string)).toEqual({ intent: "enroll", expected_version: null, token: SENTINEL });
    expect(put.credentials).toBe("same-origin");
  });

  it("claude_plan Replace sends the version the last read returned", async () => {
    setEnrollment(() => active(1, 7));
    routes["PUT /api/personal-claude/enrollment"] = () => json(200, active(2, 8));
    mount();
    await status();
    await paste(SENTINEL);
    await userEvent.click(screen.getByTestId("claude-plan-write"));
    await waitFor(() => expect(screen.getByTestId("claude-plan-status")).toHaveTextContent("generation 2"));
    expect(JSON.parse(calls("PUT", "/api/personal-claude/enrollment")[0].body as string)).toEqual({
      intent: "replace",
      expected_version: 7,
      token: SENTINEL,
    });
  });

  it("claude_plan Reconnect sends reconnect at the inactive version", async () => {
    setEnrollment(() => inactive(9));
    routes["PUT /api/personal-claude/enrollment"] = () => json(200, active(3, 10));
    mount();
    await status();
    await paste(SENTINEL);
    await userEvent.click(screen.getByTestId("claude-plan-write"));
    await waitFor(() => expect(screen.getByTestId("claude-plan-status")).toHaveTextContent("Connected"));
    expect(JSON.parse(calls("PUT", "/api/personal-claude/enrollment")[0].body as string)).toMatchObject({
      intent: "reconnect",
      expected_version: 9,
    });
  });

  it("claude_plan Disconnect asks first, then sends DELETE with the version", async () => {
    setEnrollment(() => active(1, 6));
    routes["DELETE /api/personal-claude/enrollment"] = () => json(200, inactive(7, 1, 1));
    mount();
    await status();
    await userEvent.click(screen.getByTestId("claude-plan-disconnect"));
    const dialog = screen.getByRole("dialog");
    expect(within(dialog).getByText(/stops your runs and deletes the stored token/)).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("claude-plan-disconnect-cancel"));
    expect(screen.queryByRole("dialog")).toBeNull();
    expect(fetchMock.mock.calls.some(([u]) => String(u).includes("expected_version"))).toBe(false);

    await userEvent.click(screen.getByTestId("claude-plan-disconnect"));
    await userEvent.click(screen.getByTestId("claude-plan-disconnect-confirm"));
    await waitFor(() => expect(screen.getByTestId("claude-plan-status")).toHaveTextContent("Disconnected"));
    const del = fetchMock.mock.calls.find(([, init]) => (init as RequestInit | undefined)?.method === "DELETE");
    expect(del?.[0]).toBe("/api/personal-claude/enrollment?expected_version=6");
    expect(screen.getByTestId("claude-plan-teardown")).toBeInTheDocument();
  });

  it("claude_plan refuses a malformed token without a request", async () => {
    setEnrollment(absent);
    mount();
    await status();
    await paste("two words");
    await userEvent.click(screen.getByTestId("claude-plan-write"));
    expect(await screen.findByTestId("claude-plan-error")).toHaveTextContent(/one line of visible characters/);
    expect(calls("PUT", "/api/personal-claude/enrollment")).toHaveLength(0);
    expect(tokenBox().value).toBe("");
  });

  it("claude_plan shows CLI hints from the manifest", async () => {
    setEnrollment(() => active(1, 6));
    mount();
    await status();
    expect(
      screen.getByLabelText(
        "Copy command: curie cluster subscription replace --session-file session.txt --token-file token.txt --expected-version 6",
      ),
    ).toBeInTheDocument();
    expect(
      screen.getByLabelText("Copy command: curie cluster subscription disconnect --session-file session.txt --expected-version 6"),
    ).toBeInTheDocument();
  });
});

describe("claude_plan errors", () => {
  const cases: [number, string, RegExp][] = [
    [409, "stale_version", /Your plan changed in another window\. The page has reloaded it; try again\./],
    [409, "enrollment_exists", /already connected/],
    [409, "enrollment_inactive", /disconnected/],
    [409, "enrollment_active", /still connected/],
    [409, "enrollment_absent", /No plan is connected yet/],
    [409, "staging_conflict", /still settling/],
    [503, "custody_unconfigured", /has not turned on personal Claude plans/],
    [503, "personal_rollout_in_progress", /Try again in a minute/],
    [503, "custody_unavailable", /Try again in a minute/],
    [503, "enrollment_busy", /Try again in a minute/],
    [422, "invalid_enrollment_request", /did not accept that token/],
  ];

  it.each(cases)("claude_plan %i %s gets its own sentence and clears the box", async (code, name, sentence) => {
    setEnrollment(() => active(1, 2));
    routes["PUT /api/personal-claude/enrollment"] = () => refusal(code, name);
    mount();
    await status();
    await userEvent.type(tokenBox(), SENTINEL);
    await userEvent.click(screen.getByTestId("claude-plan-write"));
    expect(await screen.findByTestId("claude-plan-error")).toHaveTextContent(sentence);
    expect(tokenBox().value).toBe("");
  });

  it("claude_plan reloads status after a 409 and sends the new version next time", async () => {
    let current = active(1, 2);
    setEnrollment(() => current);
    routes["PUT /api/personal-claude/enrollment"] = () => {
      current = active(2, 3);
      return refusal(409, "stale_version");
    };
    mount();
    await status();
    await userEvent.type(tokenBox(), SENTINEL);
    await userEvent.click(screen.getByTestId("claude-plan-write"));
    await waitFor(() => expect(screen.getByTestId("claude-plan-status")).toHaveTextContent("generation 2"));
    routes["PUT /api/personal-claude/enrollment"] = () => json(200, active(3, 4));
    await userEvent.type(tokenBox(), SENTINEL);
    await userEvent.click(screen.getByTestId("claude-plan-write"));
    await waitFor(() => expect(screen.getByTestId("claude-plan-status")).toHaveTextContent("generation 3"));
    const bodies = calls("PUT", "/api/personal-claude/enrollment").map((c) => JSON.parse(c.body as string));
    expect(bodies.map((b) => b.expected_version)).toEqual([2, 3]);
  });
});

describe("claude_plan token never echoed", () => {
  it("claude_plan token never echoed in storage, cache, toast or errors", async () => {
    let fail = false;
    let current = absent();
    setEnrollment(() => current);
    routes["PUT /api/personal-claude/enrollment"] = () => {
      if (fail) return refusal(409, "stale_version");
      current = active(1, 1);
      return json(200, current);
    };
    const logged: unknown[] = [];
    for (const level of ["log", "info", "warn", "error", "debug"] as const) {
      vi.spyOn(console, level).mockImplementation((...args: unknown[]) => void logged.push(args));
    }
    mount();
    await status();

    await userEvent.type(tokenBox(), SENTINEL);
    await userEvent.click(screen.getByTestId("claude-plan-write"));
    await waitFor(() => expect(screen.getByTestId("claude-plan-status")).toHaveTextContent("Connected"));
    expect(tokenBox().value).toBe("");

    fail = true;
    await userEvent.type(tokenBox(), SENTINEL);
    await userEvent.click(screen.getByTestId("claude-plan-write"));
    await screen.findByTestId("claude-plan-error");
    expect(tokenBox().value).toBe("");

    expect(document.body.innerHTML).not.toContain(SENTINEL);
    expect(JSON.stringify(localStorage)).not.toContain(SENTINEL);
    expect(JSON.stringify(sessionStorage)).not.toContain(SENTINEL);
    expect(JSON.stringify(queryClient.getQueryCache().getAll().map((q) => q.state))).not.toContain(SENTINEL);
    expect(JSON.stringify(queryClient.getMutationCache().getAll())).not.toContain(SENTINEL);
    expect(JSON.stringify(logged)).not.toContain(SENTINEL);
    for (const [url, init] of fetchMock.mock.calls) {
      expect(String(url)).not.toContain(SENTINEL);
      expect(JSON.stringify((init as RequestInit | undefined)?.headers ?? {})).not.toContain(SENTINEL);
    }
    vi.restoreAllMocks();
  });
});

describe("claude_plan session", () => {
  it("claude_plan operator session gets the sign-in prompt and no form", async () => {
    routes["GET /api/console/session"] = () => json(200, OPERATOR);
    routes["GET /api/console/sign-in-methods"] = () => json(200, { oidc: true });
    mount();
    expect(await screen.findByTestId("claude-plan-sign-in")).toHaveTextContent(
      "Sign in with your organization to manage your Claude plan",
    );
    expect(await screen.findByTestId("claude-plan-oidc-sign-in")).toHaveAttribute(
      "href",
      "/api/console/oidc/login?return_to=%2Fsettings%2Fclaude-plan",
    );
    expect(screen.queryByTestId("claude-plan-token")).toBeNull();
    expect(calls("GET", "/api/personal-claude/enrollment")).toHaveLength(0);
  });

  it("claude_plan operator session without organization sign-in shows no link", async () => {
    routes["GET /api/console/session"] = () => json(200, OPERATOR);
    routes["GET /api/console/sign-in-methods"] = () => json(200, { oidc: false });
    mount();
    await screen.findByTestId("claude-plan-sign-in");
    await waitFor(() => expect(calls("GET", "/api/console/sign-in-methods")).toHaveLength(1));
    expect(screen.queryByTestId("claude-plan-oidc-sign-in")).toBeNull();
  });

  it("claude_plan a load failure offers a retry", async () => {
    routes["GET /api/personal-claude/enrollment"] = () => refusal(503, "custody_unconfigured");
    mount();
    expect(await screen.findByTestId("claude-plan-load-error")).toBeInTheDocument();
    setEnrollment(absent);
    await userEvent.click(screen.getByRole("button", { name: "Retry" }));
    expect(await status()).toHaveTextContent("Not connected");
  });
});
