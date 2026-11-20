import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { StoreProvider } from "../../state/store";
import { WiredMemoryPage } from "./WiredMemoryPage";

// Handoff proof (#5800): the page runs on the real client, and only `fetch` is
// replaced. The list and every answer are the ones the real routes produced
// (tests/vectors/memory-rules-responses.json and memory-rules-console-responses.json,
// recorded and drift-checked by the API tests), so a refusal reaches the card
// exactly as the API sends it, code included.
const root = join(process.cwd(), "../..", "tests", "vectors");
const LIST = JSON.parse(readFileSync(join(root, "memory-rules-responses.json"), "utf8")).list;
const ANSWERS = JSON.parse(readFileSync(join(root, "memory-rules-console-responses.json"), "utf8")) as Record<
  string,
  { status: number; body: { detail: { code?: string; message: string } } }
>;
const PROPOSED = LIST.rules.find((r: { state: string }) => r.state === "proposed");

interface Call {
  method: string;
  path: string;
}

type Rule = { id: string; state: string; text: string };

// A small stand-in for the routes: the list answers from `rules`, and a decision
// answers with the route's recorded answer and then changes the list the way the
// route did (the acted rule becomes the recorded answer's rule, under its own id).
function serve(decision: { status: number; body: unknown }, calls: Call[], onDecide?: (id: string, action: string) => void) {
  const rules: Rule[] = structuredClone(LIST.rules);
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      calls.push({ method: init?.method ?? "GET", path });
      const json = (status: number, body: unknown) =>
        new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
      if (path === "/api/agents") {
        return json(200, [{ id: "ag-1", name: "factory", channels: [], created_at: "2026-09-01T00:00:00Z" }]);
      }
      if (path === "/api/agents/ag-1/memory/rules") return json(200, { ...LIST, rules });
      const decided = /\/memory\/rules\/([^/]+)\/(accept|reject|remove)$/.exec(path);
      if (decided && init?.method === "POST") {
        onDecide?.(decided[1], decided[2]);
        const answered = (decision.body as { rule?: Rule }).rule;
        if (decision.status === 200 && answered) {
          const at = rules.findIndex((r) => r.id === decided[1]);
          rules[at] = { ...rules[at], ...answered, id: decided[1], text: rules[at].text };
        }
        return json(decision.status, decision.body);
      }
      return json(404, { detail: "not stubbed" });
    }),
  );
}

function renderPage() {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } });
  return render(
    <QueryClientProvider client={client}>
      <StoreProvider>
        <WiredMemoryPage />
      </StoreProvider>
    </QueryClientProvider>,
  );
}

async function clickAccept() {
  const [card] = await screen.findAllByTestId("memory-rule");
  await userEvent.click(within(card).getByRole("button", { name: "Accept" }));
  return card;
}

const reads = (calls: Call[]) => calls.filter((c) => c.path === "/api/agents/ag-1/memory/rules").length;

beforeEach(() => vi.resetAllMocks());
afterEach(() => vi.unstubAllGlobals());

describe("the card shows what the real routes refuse with", () => {
  it.each([
    ["refusal_stale_version", "This rule changed. The list was refreshed."],
    [
      "refusal_write_access_required",
      "Only someone who can merge to acme-corp/acme-bot can do this. Link your code host account if you can.",
    ],
    ["refusal_code_host_unavailable", "The code host could not be reached. Nothing changed."],
  ])("%s", async (name, sentence) => {
    const calls: Call[] = [];
    serve(ANSWERS[name], calls);
    renderPage();
    await clickAccept();
    expect(await screen.findByText(sentence)).toBeInTheDocument();
    // The accept went to the proposed rule's own route, then the list was read again.
    expect(calls.filter((c) => c.method === "POST")).toEqual([
      { method: "POST", path: `/api/agents/ag-1/memory/rules/${PROPOSED.id}/accept` },
    ]);
    expect(calls.filter((c) => c.path === "/api/agents/ag-1/memory/rules")).toHaveLength(2);
  });

  it("a refusal that lost its code cannot produce the card's sentence", async () => {
    const { code: _code, ...detail } = ANSWERS.refusal_stale_version.body.detail;
    serve({ status: 409, body: { detail } }, []);
    renderPage();
    const card = await clickAccept();
    // The card falls back to the API's own message; the console's sentence needs the code.
    expect(await within(card).findByText(detail.message)).toBeInTheDocument();
    expect(screen.queryByText("This rule changed. The list was refreshed.")).toBeNull();
  });

  it.each([
    ["accept", "Accept", async () => {
      expect(screen.getAllByText(/Accepted by Acme Owner/)).toHaveLength(2);
      expect(screen.getByText("Nothing waiting for review.")).toBeInTheDocument();
    }],
    ["reject", "Dismiss", async () => {
      expect(screen.getByText("Dismissed")).toBeInTheDocument();
      expect(screen.getByText("Nothing waiting for review.")).toBeInTheDocument();
    }],
    ["remove", "Remove", async () => {
      expect(screen.getByText("Removed")).toBeInTheDocument();
      expect(screen.getByText("No rules yet.")).toBeInTheDocument();
    }],
  ] as const)("the recorded %s answer is followed by a reload that the page shows", async (name, button, shown) => {
    const calls: Call[] = [];
    serve(ANSWERS[name], calls);
    renderPage();
    const cards = await screen.findAllByTestId("memory-rule");
    // Before the action the page shows the proposed rule and the one in force.
    expect(cards).toHaveLength(2);
    expect(reads(calls)).toBe(1);

    await userEvent.click(screen.getByRole("button", { name: button }));
    await waitFor(() => expect(reads(calls)).toBe(2));
    await waitFor(shown);
    expect(calls.filter((c) => c.method === "POST")).toEqual([
      { method: "POST", path: expect.stringMatching(new RegExp(`/memory/rules/[^/]+/${name}$`)) },
    ]);
    expect(screen.queryByTestId("memory-rule-refusal")).toBeNull();
  });
});
