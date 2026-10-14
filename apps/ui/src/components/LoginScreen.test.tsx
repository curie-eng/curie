import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { StoreProvider } from "../state/store";
import { LoginScreen } from "./LoginScreen";

// The login screen posts through the real client (exchangeConsoleLoginCode) to
// a stubbed `fetch`, so the request body and error mapping are the production
// path. Responses mirror the API verbatim:
//  - apps/api/src/curie_api/schemas.py ConsoleSessionOut: {subject, expires_at}
//  - apps/api/src/curie_api/routers/console.py POST /console/session: 401
//    "invalid or expired login code"
//  - apps/api/src/curie_api/rate_limit.py: 429 "rate limit exceeded" with
//    Retry-After, 503 "rate limiter unavailable"

const EXPIRES_AT = "2026-07-24T12:00:00+00:00";

function json(status: number, body: unknown, headers: Record<string, string> = {}): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json", ...headers },
  });
}

const ok = (subject: string | null = "U0EXCHANGED") => json(200, { subject, expires_at: EXPIRES_AT });
const badCode = () => json(401, { detail: "invalid or expired login code" });
const rateLimited = () =>
  json(429, { detail: "rate limit exceeded" }, { "Retry-After": "60", "Cache-Control": "no-store" });
const limiterDown = () => json(503, { detail: "rate limiter unavailable" }, { "Cache-Control": "no-store" });

const RATE_LIMIT_MESSAGE = "Too many sign-in attempts. Wait a minute and try again.";
const UNAVAILABLE_MESSAGE = "Console sign-in is temporarily unavailable. Try again shortly.";

let fetchMock: ReturnType<typeof vi.fn>;

beforeEach(() => {
  fetchMock = vi.fn();
  vi.stubGlobal("fetch", fetchMock);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function renderLogin(notice: string | null = null) {
  const onSignedIn = vi.fn();
  render(
    <StoreProvider>
      <LoginScreen notice={notice} onSignedIn={onSignedIn} />
    </StoreProvider>,
  );
  return { onSignedIn };
}

function postedCodes(): string[] {
  return fetchMock.mock.calls
    .filter(([u, init]) => u === "/api/console/session" && (init as RequestInit | undefined)?.method === "POST")
    .map(([, init]) => (JSON.parse(String((init as RequestInit).body)) as { code: string }).code);
}

async function submitCode(code: string) {
  const input = screen.getByLabelText("login code");
  await userEvent.clear(input);
  await userEvent.type(input, code);
  await userEvent.click(screen.getByTestId("console-login-submit"));
}

describe("LoginScreen (#1047)", () => {
  it("renders the code form and both CLI hints resolved from the manifest", () => {
    renderLogin();
    const root = screen.getByTestId("console-login");
    expect(root).toHaveTextContent("Sign in to Curie Console");
    const input = screen.getByLabelText("login code");
    expect(input).toHaveAttribute("autocomplete", "one-time-code");
    expect(screen.getByTestId("console-login-submit")).toHaveTextContent("Sign in");
    expect(screen.getByRole("button", { name: /curie local console login --subject/ })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /curie cluster console login --subject/ })).toBeInTheDocument();
    expect(root).not.toHaveTextContent(/api_key/i);
    expect(screen.queryByTestId("console-login-notice")).not.toBeInTheDocument();
    expect(screen.queryByTestId("console-login-error")).not.toBeInTheDocument();
  });

  it("renders the notice it is given", () => {
    renderLogin("Your console session expired or was revoked. Mint a new login code and sign in again.");
    expect(screen.getByTestId("console-login-notice")).toHaveTextContent("expired or was revoked");
  });

  it.each([
    ["empty", ""],
    ["whitespace-only", "   "],
  ])("refuses an %s code client-side and sends nothing", async (_label, code) => {
    const { onSignedIn } = renderLogin();
    if (code) await userEvent.type(screen.getByLabelText("login code"), code);
    await userEvent.click(screen.getByTestId("console-login-submit"));

    expect(await screen.findByTestId("console-login-error")).toHaveTextContent(
      "Enter a login code minted by the Curie CLI.",
    );
    expect(fetchMock).not.toHaveBeenCalled();
    expect(onSignedIn).not.toHaveBeenCalled();
  });

  // Liveness for the refusal above: a real code with stray whitespace from a
  // terminal copy is sent, trimmed, as the exchange's only field.
  it("exchanges a pasted code, trimmed, and hands the session to onSignedIn", async () => {
    fetchMock.mockResolvedValue(ok("U0EXCHANGED"));
    const { onSignedIn } = renderLogin();
    await userEvent.type(screen.getByLabelText("login code"), "  one-time-example-code ");
    await userEvent.click(screen.getByTestId("console-login-submit"));

    await waitFor(() => expect(onSignedIn).toHaveBeenCalledTimes(1));
    expect(onSignedIn).toHaveBeenCalledWith({ subject: "U0EXCHANGED", expires_at: EXPIRES_AT });
    const [requestUrl, init] = fetchMock.mock.calls[0] as [string, RequestInit];
    expect(requestUrl).toBe("/api/console/session");
    expect(init.method).toBe("POST");
    expect(init.credentials).toBe("same-origin");
    expect(JSON.parse(String(init.body))).toEqual({ code: "one-time-example-code" });
    expect(Object.keys((init.headers as Record<string, string>) ?? {}).some((h) => /^x-api-key$/i.test(h))).toBe(false);
    expect(screen.getByLabelText("login code")).toHaveValue("");
  });

  it("submits on Enter", async () => {
    fetchMock.mockResolvedValue(ok());
    const { onSignedIn } = renderLogin();
    await userEvent.type(screen.getByLabelText("login code"), "one-time-example-code{Enter}");
    await waitFor(() => expect(onSignedIn).toHaveBeenCalledTimes(1));
    expect(postedCodes()).toEqual(["one-time-example-code"]);
  });

  it("disables submit and shows progress while the exchange is in flight", async () => {
    let answer!: (r: Response) => void;
    fetchMock.mockReturnValue(
      new Promise<Response>((r) => {
        answer = r;
      }),
    );
    const { onSignedIn } = renderLogin();
    await submitCode("one-time-example-code");

    const submit = screen.getByTestId("console-login-submit");
    expect(submit).toBeDisabled();
    expect(submit).toHaveTextContent("Signing in…");

    answer(ok());
    await waitFor(() => expect(onSignedIn).toHaveBeenCalledTimes(1));
  });

  it.each([
    ["401 bad code", badCode, "invalid or expired login code"],
    ["429 rate limited", rateLimited, RATE_LIMIT_MESSAGE],
    ["503 limiter down", limiterDown, UNAVAILABLE_MESSAGE],
    [
      "500 other failure",
      () => new Response("Internal Server Error", { status: 500, statusText: "Internal Server Error" }),
      "500: Internal Server Error",
    ],
  ])("shows the %s message and stays on the form", async (_label, refusal, message) => {
    fetchMock.mockResolvedValue(refusal());
    const { onSignedIn } = renderLogin();
    await submitCode("one-time-example-code");

    expect(await screen.findByTestId("console-login-error")).toHaveTextContent(message);
    expect(onSignedIn).not.toHaveBeenCalled();
    expect(screen.getByTestId("console-login")).toBeInTheDocument();
    expect(screen.getByTestId("console-login-submit")).toBeEnabled();
  });

  it("shows a network failure's message", async () => {
    fetchMock.mockRejectedValue(new TypeError("Failed to fetch"));
    const { onSignedIn } = renderLogin();
    await submitCode("one-time-example-code");
    expect(await screen.findByTestId("console-login-error")).toHaveTextContent("Failed to fetch");
    expect(onSignedIn).not.toHaveBeenCalled();
  });

  it("treats a 200 with a blank subject as a failed sign-in", async () => {
    fetchMock.mockResolvedValue(ok(null));
    const { onSignedIn } = renderLogin();
    await submitCode("one-time-example-code");
    expect(await screen.findByTestId("console-login-error")).toHaveTextContent(
      "missing, invalid, or expired console session",
    );
    expect(onSignedIn).not.toHaveBeenCalled();
  });

  it("recovers on the same form after a 401, a 429 and a 503", async () => {
    fetchMock
      .mockResolvedValueOnce(badCode())
      .mockResolvedValueOnce(rateLimited())
      .mockResolvedValueOnce(limiterDown())
      .mockResolvedValueOnce(ok("U0RECOVERED"));
    const { onSignedIn } = renderLogin();

    await submitCode("first-example-code");
    expect(await screen.findByTestId("console-login-error")).toHaveTextContent("invalid or expired login code");
    expect(screen.getByTestId("console-login-submit")).toBeEnabled();

    await submitCode("second-example-code");
    expect(await screen.findByText(RATE_LIMIT_MESSAGE)).toBeInTheDocument();
    expect(screen.getByTestId("console-login-submit")).toBeEnabled();

    await submitCode("third-example-code");
    expect(await screen.findByText(UNAVAILABLE_MESSAGE)).toBeInTheDocument();
    expect(screen.getByTestId("console-login-submit")).toBeEnabled();

    await submitCode("fourth-example-code");
    await waitFor(() => expect(onSignedIn).toHaveBeenCalledTimes(1));
    expect(onSignedIn).toHaveBeenCalledWith({ subject: "U0RECOVERED", expires_at: EXPIRES_AT });
    expect(screen.queryByTestId("console-login-error")).not.toBeInTheDocument();
    expect(postedCodes()).toEqual([
      "first-example-code",
      "second-example-code",
      "third-example-code",
      "fourth-example-code",
    ]);
  });
});
