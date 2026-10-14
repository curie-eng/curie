import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { StoreProvider } from "./store";
import { ConsoleSessionGate, useConsoleSession } from "./session";
import { getAgents } from "../api/client";

// The gate is exercised through the real client over a stubbed `fetch`, so the
// probe, the login exchange, and the unauthorized notification all run their
// production paths. Response bodies mirror the API verbatim:
//  - apps/api/src/curie_api/schemas.py ConsoleSessionOut: {subject, expires_at}
//  - apps/api/src/curie_api/routers/console.py: 401 "missing, invalid, or
//    expired console session" on GET, 401 "invalid or expired login code" on POST
//  - apps/api/src/curie_api/rate_limit.py: 429 "rate limit exceeded" with
//    Retry-After, 503 "rate limiter unavailable"
//  - apps/api/src/curie_api/auth.py require_api_key: 401 "missing or invalid API key"
//  - apps/api/src/curie_api/routers/state.py: 401 "missing or invalid credential"

const EXPIRES_AT = "2026-07-24T12:00:00+00:00";
const SESSION_401 = { detail: "missing, invalid, or expired console session" };

function json(status: number, body: unknown, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

const rateLimited = () => json(429, { detail: "rate limit exceeded" }, { "Retry-After": "60", "Cache-Control": "no-store" });
const limiterDown = () => json(503, { detail: "rate limiter unavailable" }, { "Cache-Control": "no-store" });
const signedInAs = (subject: string | null) => () => json(200, { subject, expires_at: EXPIRES_AT });

type Handler = (init: RequestInit) => Response | Promise<Response>;

// A mutable route table keyed "METHOD /path". Tests flip entries mid-test.
let routes: Record<string, Handler>;
let fetchMock: ReturnType<typeof vi.fn>;

function sessionProbes(): number {
  return fetchMock.mock.calls.filter(
    ([u, init]) => u === "/api/console/session" && ((init as RequestInit | undefined)?.method ?? "GET") === "GET",
  ).length;
}

function deferred<T>() {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

beforeEach(() => {
  routes = {};
  fetchMock = vi.fn((input: string, init: RequestInit = {}) => {
    const key = `${(init.method ?? "GET").toUpperCase()} ${input}`;
    const handler = routes[key];
    return Promise.resolve(handler ? handler(init) : json(404, { detail: "Not Found" }));
  });
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

// A console child that shows who is signed in and can send one real request
// (GET /agents) through the client. Its local state survives only while it
// stays mounted, which is how the tests tell "kept in" from "remounted".
function Console() {
  const session = useConsoleSession();
  const [result, setResult] = useState<string | null>(null);
  const load = (n: number) => {
    void Promise.allSettled(Array.from({ length: n }, () => getAgents())).then((outcomes) => {
      const first = outcomes[0];
      setResult(
        first.status === "fulfilled"
          ? "loaded"
          : `failed ${(first.reason as { status?: number }).status ?? "?"}`,
      );
    });
  };
  return (
    <div data-testid="console-child">
      <span data-testid="whoami">{session.subject}</span>
      <button type="button" onClick={() => load(1)}>
        load agents
      </button>
      <button type="button" onClick={() => load(2)}>
        load agents twice
      </button>
      {result ? <span data-testid="child-result">{result}</span> : null}
    </div>
  );
}

function renderGate() {
  return render(
    <StoreProvider>
      <ConsoleSessionGate>
        <Console />
      </ConsoleSessionGate>
    </StoreProvider>,
  );
}

async function renderSignedIn(subject = "U0AUTHENTICATED") {
  routes["GET /api/console/session"] = signedInAs(subject);
  const view = renderGate();
  expect(await screen.findByTestId("whoami")).toHaveTextContent(subject);
  return view;
}

describe("ConsoleSessionGate: initial probe (#1047)", () => {
  it("renders the console with the session subject when GET /console/session is 200", async () => {
    await renderSignedIn("U0AUTHENTICATED");
    expect(screen.queryByTestId("console-login")).not.toBeInTheDocument();
    expect(sessionProbes()).toBe(1);
  });

  it("renders the login screen, without a notice and without the console, when the probe is 401", async () => {
    routes["GET /api/console/session"] = () => json(401, SESSION_401);
    routes["GET /api/agents"] = () => json(200, []);
    renderGate();

    expect(await screen.findByTestId("console-login")).toBeInTheDocument();
    expect(screen.queryByTestId("console-login-notice")).not.toBeInTheDocument();
    expect(screen.queryByTestId("console-child")).not.toBeInTheDocument();
    // Nothing behind the gate ran: no protected request was sent.
    expect(fetchMock.mock.calls.some(([u]) => u === "/api/agents")).toBe(false);
  });

  it.each([
    ["an empty string", ""],
    ["whitespace", "   "],
    ["null (historical row)", null],
  ])("treats a 200 with a blank subject (%s) as signed out", async (_label, subject) => {
    routes["GET /api/console/session"] = signedInAs(subject);
    renderGate();
    expect(await screen.findByTestId("console-login")).toBeInTheDocument();
    expect(screen.queryByTestId("console-child")).not.toBeInTheDocument();
  });

  it.each([
    [429, rateLimited, /too many requests/i],
    [503, limiterDown, /temporarily unavailable/i],
  ])("a %i probe shows the unavailable state, not the code form; Retry recovers", async (_status, refusal, message) => {
    routes["GET /api/console/session"] = refusal;
    renderGate();

    const unavailable = await screen.findByTestId("console-session-unavailable");
    expect(unavailable).toHaveTextContent(message);
    expect(screen.queryByTestId("console-login")).not.toBeInTheDocument();
    expect(screen.queryByTestId("console-child")).not.toBeInTheDocument();

    routes["GET /api/console/session"] = signedInAs("U0AUTHENTICATED");
    await userEvent.click(screen.getByRole("button", { name: "Retry" }));

    expect(await screen.findByTestId("whoami")).toHaveTextContent("U0AUTHENTICATED");
    expect(screen.queryByTestId("console-session-unavailable")).not.toBeInTheDocument();
    expect(sessionProbes()).toBe(2);
  });

  it("signing in from the login screen enters the console as the exchanged subject", async () => {
    routes["GET /api/console/session"] = () => json(401, SESSION_401);
    routes["POST /api/console/session"] = signedInAs("U0EXCHANGED");
    renderGate();

    await userEvent.type(await screen.findByLabelText("login code"), "one-time-example-code");
    await userEvent.click(screen.getByTestId("console-login-submit"));

    expect(await screen.findByTestId("whoami")).toHaveTextContent("U0EXCHANGED");
    expect(screen.queryByTestId("console-login")).not.toBeInTheDocument();
    const post = fetchMock.mock.calls.find(
      ([u, init]) => u === "/api/console/session" && (init as RequestInit | undefined)?.method === "POST",
    );
    expect(JSON.parse(String((post![1] as RequestInit).body))).toEqual({ code: "one-time-example-code" });
  });

  it("useConsoleSession outside a signed-in gate throws", () => {
    const quiet = vi.spyOn(console, "error").mockImplementation(() => {});
    try {
      expect(() =>
        render(
          <StoreProvider>
            <Console />
          </StoreProvider>,
        ),
      ).toThrow("useConsoleSession must be used within ConsoleSessionGate");
    } finally {
      quiet.mockRestore();
    }
  });
});

describe("ConsoleSessionGate: a 401 mid-session (#1047)", () => {
  it("returns to the login screen with the expiry notice when the re-probe is 401, then signs back in", async () => {
    await renderSignedIn();
    routes["GET /api/agents"] = () => json(401, { detail: "missing or invalid API key" });
    routes["GET /api/console/session"] = () => json(401, SESSION_401);

    await userEvent.click(screen.getByRole("button", { name: "load agents" }));

    expect(await screen.findByTestId("console-login-notice")).toHaveTextContent("expired or was revoked");
    expect(screen.getByTestId("console-login")).toBeInTheDocument();
    expect(screen.queryByTestId("console-child")).not.toBeInTheDocument();

    // Recovery: a fresh code puts the operator back in the console.
    routes["POST /api/console/session"] = signedInAs("U0AUTHENTICATED");
    routes["GET /api/agents"] = () => json(200, []);
    await userEvent.type(screen.getByLabelText("login code"), "fresh-example-code");
    await userEvent.click(screen.getByTestId("console-login-submit"));
    expect(await screen.findByTestId("whoami")).toHaveTextContent("U0AUTHENTICATED");
    await userEvent.click(screen.getByRole("button", { name: "load agents" }));
    expect(await screen.findByTestId("child-result")).toHaveTextContent("loaded");
  });

  it("two concurrent 401s send exactly one re-probe", async () => {
    await renderSignedIn();
    expect(sessionProbes()).toBe(1);
    const probe = deferred<Response>();
    routes["GET /api/agents"] = () => json(401, { detail: "missing or invalid API key" });
    routes["GET /api/console/session"] = () => probe.promise;

    await userEvent.click(screen.getByRole("button", { name: "load agents twice" }));
    await waitFor(() => expect(screen.getByTestId("child-result")).toHaveTextContent("failed 401"));
    await waitFor(() => expect(sessionProbes()).toBe(2));

    probe.resolve(json(401, SESSION_401));
    expect(await screen.findByTestId("console-login-notice")).toHaveTextContent("expired or was revoked");
    expect(sessionProbes()).toBe(2);
  });

  // Liveness for the refusal above: some routes 401 a live session by design
  // (the state router refuses console sessions). A live re-probe, or a probe
  // the limiter refused, must leave the operator where they are.
  it.each([
    ["200 (session still live)", signedInAs("U0AUTHENTICATED")],
    ["429 (rate limited)", rateLimited],
    ["503 (limiter down)", limiterDown],
  ])("a route-specific 401 keeps the user in when the re-probe is %s", async (_label, probeResponse) => {
    await renderSignedIn();
    routes["GET /api/agents"] = () => json(401, { detail: "missing or invalid credential" });

    const lastProbe: { resp: Response | null } = { resp: null };
    routes["GET /api/console/session"] = () => {
      lastProbe.resp = probeResponse();
      return lastProbe.resp;
    };

    await userEvent.click(screen.getByRole("button", { name: "load agents" }));
    await waitFor(() => expect(sessionProbes()).toBe(2));
    // Wait until the gate has read the probe's answer, then let React settle.
    await waitFor(() => expect(lastProbe.resp?.bodyUsed).toBe(true));
    await act(async () => {
      await new Promise((r) => setTimeout(r, 0));
    });

    // The child kept its local state, so it was never unmounted.
    expect(screen.getByTestId("child-result")).toHaveTextContent("failed 401");
    expect(screen.queryByTestId("console-login")).not.toBeInTheDocument();
    expect(screen.queryByTestId("console-session-unavailable")).not.toBeInTheDocument();
    expect(screen.getByTestId("whoami")).toHaveTextContent("U0AUTHENTICATED");
  });

  it("a 403 or 500 from a protected call does not re-probe the session", async () => {
    await renderSignedIn();
    routes["GET /api/agents"] = () => json(403, { detail: "console session origin rejected" });
    await userEvent.click(screen.getByRole("button", { name: "load agents" }));
    expect(await screen.findByTestId("child-result")).toHaveTextContent("failed 403");

    routes["GET /api/agents"] = () => json(500, { detail: "boom" });
    await userEvent.click(screen.getByRole("button", { name: "load agents" }));
    await waitFor(() => expect(screen.getByTestId("child-result")).toHaveTextContent("failed 500"));

    expect(sessionProbes()).toBe(1);
    expect(screen.getByTestId("whoami")).toBeInTheDocument();
  });

  it("stops listening after unmount", async () => {
    const view = await renderSignedIn();
    view.unmount();
    routes["GET /api/agents"] = () => json(401, { detail: "missing or invalid API key" });

    await expect(getAgents()).rejects.toMatchObject({ status: 401 });
    expect(sessionProbes()).toBe(1);
  });
});
