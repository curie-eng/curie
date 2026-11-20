import { beforeEach, describe, expect, it, vi } from "vitest";
import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { StoreProvider } from "../../state/store";
import { WiredMemoryPage, authorOf, pullRequestNumberOf, utcWhen } from "./WiredMemoryPage";
import {
  acceptMemoryRule,
  getAgents,
  listMemory,
  listMemoryRules,
  rejectMemoryRule,
  removeMemoryRule,
  MemoryRuleRefusal,
  type AgentOut,
  type MemoryRule,
  type MemoryRules,
} from "../../api/client";

// Mock only the data layer; the real page, the real store and the real refusal
// class run. The rules are the shapes recorded from the real routes
// (tests/vectors/memory-rules-responses.json, drift-checked by the API tests).
vi.mock("../../api/client", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/client")>();
  return {
    ...actual,
    getAgents: vi.fn(),
    listMemory: vi.fn(),
    listMemoryRules: vi.fn(),
    acceptMemoryRule: vi.fn(),
    rejectMemoryRule: vi.fn(),
    removeMemoryRule: vi.fn(),
  };
});

const VECTOR = JSON.parse(
  readFileSync(join(process.cwd(), "../../tests/vectors/memory-rules-responses.json"), "utf8"),
) as { list: MemoryRules };

const [PROPOSED, ACCEPTED] = VECTOR.list.rules as [MemoryRule, MemoryRule];
const REPO = "acme-corp/acme-bot";

const agent = (id: string, name: string): AgentOut =>
  ({ id, name, channels: [], created_at: "2026-09-01T00:00:00Z" }) as unknown as AgentOut;
const AGENTS = [agent("ag-1", "factory"), agent("ag-2", "support")];

const rule = (over: Partial<MemoryRule>): MemoryRule => ({ ...PROPOSED, ...over });
const list = (...rules: MemoryRule[]): MemoryRules => ({
  rules,
  scopes: [],
  in_force_limit: 16,
});

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

const refusal = (status: number, code: string, message = "api sentence", limit: number | null = null) =>
  new MemoryRuleRefusal(status, code, message, limit);

beforeEach(() => {
  vi.resetAllMocks();
  vi.mocked(getAgents).mockResolvedValue(AGENTS);
  vi.mocked(listMemory).mockResolvedValue([]);
});

describe("WiredMemoryPage layout (#5800)", () => {
  it("opens on Learned rules with the heading, agent selector and sub-tabs", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(list(PROPOSED, ACCEPTED));
    renderPage();

    expect(await screen.findByRole("heading", { name: "Memory", level: 1 })).toBeInTheDocument();
    expect(screen.getByTestId("memory-agent-select")).toBeInTheDocument();
    const group = screen.getByRole("group", { name: "Memory sections" });
    expect(within(group).getByRole("button", { name: "Learned rules" })).toHaveAttribute("aria-pressed", "true");
    expect(within(group).getByRole("button", { name: "Facts" })).toHaveAttribute("aria-pressed", "false");
    expect(listMemoryRules).toHaveBeenCalledWith("ag-1");
  });

  it("shows the proposed and the accepted rule of one repository", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(
      list(
        rule({ state: "proposed", text: "run the formatter", proposed_by: "github:6601:octocat" }),
        rule({
          id: "r2",
          state: "accepted",
          text: "keep PRs small",
          accepted_by: { principal_id: "p1", display_name: "Acme Owner" },
          accepted_at: "2026-11-18T10:31:42Z",
        }),
        // A rejected and a removed rule are final and never shown.
        rule({ id: "r3", state: "rejected", text: "gone one" }),
        rule({ id: "r4", state: "removed", text: "gone two" }),
      ),
    );
    renderPage();

    expect(
      await screen.findByText(
        `What this factory has learned on ${REPO}. A rule applies only after someone who can merge here accepts it.`,
      ),
    ).toBeInTheDocument();
    expect(screen.getByText(REPO)).toBeInTheDocument(); // the chip: one repository
    expect(screen.queryByTestId("memory-repository-select")).toBeNull();

    const sections = screen.getAllByRole("heading", { level: 2 });
    expect(sections.map((h) => h.textContent)).toEqual(["Proposed", "In force", "What wins"]);
    const [proposedCard, acceptedCard] = screen.getAllByTestId("memory-rule");
    expect(screen.queryByText("gone one")).toBeNull();
    expect(screen.queryByText("gone two")).toBeNull();

    expect(within(proposedCard).getByText("run the formatter")).toBeInTheDocument();
    const source = within(proposedCard).getByRole("link", { name: "octocat's comment on #12" });
    expect(source).toHaveAttribute("href", "https://github.com/acme-corp/acme-bot/pull/12");
    expect(within(proposedCard).getByText(/Why this is a rule and not a one-off: Reviewers asked for it twice\./)).toBeInTheDocument();
    expect(within(proposedCard).getByText("Scope: this repository")).toBeInTheDocument();
    expect(within(proposedCard).getByText("Applies to: every factory run here, for everyone")).toBeInTheDocument();
    expect(within(proposedCard).getByRole("button", { name: "Accept" })).toBeEnabled();
    expect(within(proposedCard).getByRole("button", { name: "Dismiss" })).toBeEnabled();

    expect(within(acceptedCard).getByText("keep PRs small")).toBeInTheDocument();
    expect(
      within(acceptedCard).getByText(
        /Accepted by Acme Owner, who can merge to this repository, on 2026-11-18 10:31 UTC/,
      ),
    ).toBeInTheDocument();
    expect(within(acceptedCard).getByRole("button", { name: "Remove" })).toBeInTheDocument();
    // The counts sit in the section headers.
    expect(within(screen.getByRole("heading", { name: "Proposed" }).parentElement!).getByText("1")).toBeInTheDocument();
    expect(within(screen.getByRole("heading", { name: "In force" }).parentElement!).getByText("1")).toBeInTheDocument();
  });

  it("shows both empty texts when a repository has no live rule", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(list(rule({ state: "rejected" })));
    renderPage();
    expect(await screen.findByText("Nothing waiting for review.")).toBeInTheDocument();
    expect(screen.getByText("No rules yet.")).toBeInTheDocument();
  });

  it("treats an author that is not github:<id>:<login> as unknown even with a valid source", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(
      list(rule({ proposed_by: "gitlab:7:someone", source_url: "https://github.com/acme-corp/acme-bot/pull/12" })),
    );
    renderPage();
    expect(await screen.findByRole("link", { name: "the source comment" })).toBeInTheDocument();
  });

  it("falls back to 'the source comment' when the author or the number cannot be parsed", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(
      list(rule({ proposed_by: "someone", source_url: "https://example.test/comment" })),
    );
    renderPage();
    expect(await screen.findByRole("link", { name: "the source comment" })).toHaveAttribute(
      "href",
      "https://example.test/comment",
    );
  });

  it("never links a source that is not http or https", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(
      list(rule({ source_url: "javascript:alert(1)" })),
    );
    renderPage();
    await screen.findByText(/Learned from/);
    expect(screen.queryByRole("link", { name: /source comment|comment on/ })).toBeNull();
  });

  it("shows a repository selector when the agent has rules in more than one repository", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(
      list(
        rule({ text: "first repo rule" }),
        rule({ id: "r9", scope_key: "github:github.com:9", scope_label: "acme-corp/other", text: "second repo rule" }),
      ),
    );
    renderPage();
    const select = await screen.findByTestId("memory-repository-select");
    expect(within(select).getAllByRole("option").map((o) => o.textContent)).toEqual([REPO, "acme-corp/other"]);
    expect(screen.getByText("first repo rule")).toBeInTheDocument();
    expect(screen.queryByText("second repo rule")).toBeNull();

    await userEvent.selectOptions(select, "github:github.com:9");
    expect(await screen.findByText("second repo rule")).toBeInTheDocument();
    expect(screen.queryByText("first repo rule")).toBeNull();
  });

  it("says no repository has learned rules when only other scopes exist", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(list(rule({ scope_kind: "agent", scope_key: "agent", scope_label: "agent" })));
    renderPage();
    expect(await screen.findByText("No repository has learned rules for this agent yet.")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "What wins" })).toBeInTheDocument();
  });

  it("reloads the rules of the agent that is selected", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(list(PROPOSED));
    renderPage();
    await userEvent.selectOptions(await screen.findByTestId("memory-agent-select"), "ag-2");
    await waitFor(() => expect(listMemoryRules).toHaveBeenCalledWith("ag-2"));
  });

  it("shows the order of authority", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(list());
    renderPage();
    expect(await screen.findByRole("heading", { name: "What wins" })).toBeInTheDocument();
    expect(screen.getByText("strongest first")).toBeInTheDocument();
    const items = within(screen.getByRole("list")).getAllByRole("listitem");
    expect(items.map((li) => li.textContent)).toEqual([
      "Platform and bundle rules",
      "The repository's own instruction files",
      "Learned rules for this repository managed here",
      "Your personal coding instructions set per developer, never override a repository rule",
    ]);
    expect(screen.getByText("Like a project file winning over a personal one.")).toBeInTheDocument();
  });

  it("renders the CLI hint for listing rules from the manifest", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(list());
    renderPage();
    await screen.findByRole("heading", { name: "What wins" });
    expect(screen.getAllByRole("button", { name: /curie cluster memory factory --rules/ }).length).toBeGreaterThan(0);
  });
});

describe("WiredMemoryPage actions (#5800)", () => {
  const proposed = () => rule({ id: "rp", version: 4, text: "a proposed rule" });
  const accepted = () =>
    rule({
      id: "ra",
      version: 7,
      state: "accepted",
      text: "an accepted rule",
      accepted_by: { principal_id: "p1", display_name: "Acme Owner" },
      accepted_at: "2026-11-18T10:31:00Z",
    });

  it("accepts a proposed rule with its version and reloads", async () => {
    vi.mocked(listMemoryRules)
      .mockResolvedValueOnce(list(proposed()))
      .mockResolvedValue(list({ ...accepted(), id: "rp", text: "a proposed rule", version: 5 }));
    vi.mocked(acceptMemoryRule).mockResolvedValue({ rule: accepted(), applies_from: "next_ticket" });
    renderPage();

    await userEvent.click(await screen.findByRole("button", { name: "Accept" }));
    await waitFor(() => expect(acceptMemoryRule).toHaveBeenCalledWith("ag-1", "rp", 4));
    expect(await screen.findByText(/Accepted by Acme Owner/)).toBeInTheDocument();
    expect(screen.getByText("Nothing waiting for review.")).toBeInTheDocument();
  });

  it("dismisses a proposed rule and leaves the note 'Dismissed' with no undo", async () => {
    vi.mocked(listMemoryRules)
      .mockResolvedValueOnce(list(proposed()))
      .mockResolvedValue(list({ ...proposed(), state: "rejected" }));
    vi.mocked(rejectMemoryRule).mockResolvedValue({ rule: proposed(), applies_from: "next_ticket" });
    renderPage();

    await userEvent.click(await screen.findByRole("button", { name: "Dismiss" }));
    await waitFor(() => expect(rejectMemoryRule).toHaveBeenCalledWith("ag-1", "rp", 4));
    expect(await screen.findByText("Dismissed")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /undo|restore/i })).toBeNull();
  });

  it("removes an accepted rule and leaves the note 'Removed'", async () => {
    vi.mocked(listMemoryRules)
      .mockResolvedValueOnce(list(accepted()))
      .mockResolvedValue(list({ ...accepted(), state: "removed" }));
    vi.mocked(removeMemoryRule).mockResolvedValue({ rule: accepted(), applies_from: "next_ticket" });
    renderPage();

    await userEvent.click(await screen.findByRole("button", { name: "Remove" }));
    await waitFor(() => expect(removeMemoryRule).toHaveBeenCalledWith("ag-1", "ra", 7));
    expect(await screen.findByText("Removed")).toBeInTheDocument();
    expect(screen.getByText("No rules yet.")).toBeInTheDocument();
  });

  const CASES: [string, MemoryRuleRefusal, string][] = [
    [
      "403 write access required",
      refusal(403, "memory_rules.write_access_required"),
      `Only someone who can merge to ${REPO} can do this. Link your code host account if you can.`,
    ],
    [
      "403 write access unverifiable shows the API's own message",
      refusal(403, "memory_rules.write_access_unverifiable", "Your linked accounts disagree on write access."),
      "Your linked accounts disagree on write access.",
    ],
    ["409 stale version", refusal(409, "memory_rules.stale_version"), "This rule changed. The list was refreshed."],
    [
      "409 scope full",
      refusal(409, "memory_rules.scope_full", "full", 7),
      `${REPO} already has 7 rules in force. Remove one first.`,
    ],
    [
      "409 scope full without a limit in the refusal uses the list's limit",
      refusal(409, "memory_rules.scope_full", "full", null),
      `${REPO} already has 16 rules in force. Remove one first.`,
    ],
    ["503 code host", refusal(503, "memory_rules.code_host_unavailable"), "The code host could not be reached. Nothing changed."],
    ["503 unavailable", refusal(503, "memory_rules.unavailable"), "The code host could not be reached. Nothing changed."],
  ];

  it.each(CASES)("%s: the card shows the message and the page stays usable", async (_name, error, text) => {
    vi.mocked(listMemoryRules).mockResolvedValue(list(proposed(), accepted()));
    vi.mocked(acceptMemoryRule).mockRejectedValue(error);
    renderPage();

    const cards = await screen.findAllByTestId("memory-rule");
    await userEvent.click(within(cards[0]).getByRole("button", { name: "Accept" }));
    expect(await within((await screen.findAllByTestId("memory-rule"))[0]).findByText(text)).toBeInTheDocument();
    // The rest of the page still works: no page level error, both cards and buttons present.
    expect(screen.queryByTestId("memory-rules-error")).toBeNull();
    expect(screen.getAllByTestId("memory-rule")).toHaveLength(2);
    expect(screen.getByRole("button", { name: "Remove" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Dismiss" })).toBeEnabled();
    // Every refusal reloads the list.
    expect(vi.mocked(listMemoryRules).mock.calls.length).toBeGreaterThanOrEqual(2);
  });

  it("keeps the refusal when the refreshed list no longer shows the rule", async () => {
    // Another person dismissed the proposal after this page loaded: the accept is
    // refused as stale, and the reload takes the rule out of Proposed.
    vi.mocked(listMemoryRules)
      .mockResolvedValueOnce(list(proposed()))
      .mockResolvedValue(list({ ...proposed(), state: "rejected", version: 5 }));
    vi.mocked(acceptMemoryRule).mockRejectedValue(refusal(409, "memory_rules.stale_version"));
    renderPage();

    await userEvent.click(await screen.findByRole("button", { name: "Accept" }));
    expect(await screen.findByText("Nothing waiting for review.")).toBeInTheDocument();
    expect(screen.getByTestId("memory-rule-refusal")).toHaveTextContent("This rule changed. The list was refreshed.");
  });

  it("applies only the newest reload when two decisions overlap", async () => {
    const a = rule({ id: "ra", text: "rule a", version: 1 });
    const b = rule({ id: "rb", text: "rule b", version: 1 });
    const acceptedA = { ...a, state: "accepted" as const, version: 2, accepted_by: { principal_id: "p", display_name: "Acme Owner" }, accepted_at: "2026-11-18T10:31:00Z" };
    const acceptedB = { ...b, state: "accepted" as const, version: 2, accepted_by: { principal_id: "p", display_name: "Acme Owner" }, accepted_at: "2026-11-18T10:32:00Z" };
    let releaseOlder!: (v: MemoryRules) => void;
    const older = new Promise<MemoryRules>((resolve) => (releaseOlder = resolve));
    vi.mocked(listMemoryRules)
      .mockResolvedValueOnce(list(a, b))
      .mockReturnValueOnce(older) // the reload after the first decision, read before the second landed
      .mockResolvedValue(list(acceptedA, acceptedB)); // the reload after the second decision
    vi.mocked(acceptMemoryRule).mockResolvedValue({ rule: acceptedA, applies_from: "next_ticket" });
    renderPage();

    const cards = await screen.findAllByTestId("memory-rule");
    await userEvent.click(within(cards[0]).getByRole("button", { name: "Accept" }));
    // The other card stays usable while the first decision's reload is pending.
    expect(within(screen.getAllByTestId("memory-rule")[1]).getByRole("button", { name: "Accept" })).toBeEnabled();
    await userEvent.click(within(screen.getAllByTestId("memory-rule")[1]).getByRole("button", { name: "Accept" }));
    await waitFor(() => expect(screen.getAllByText(/Accepted by Acme Owner/)).toHaveLength(2));
    // The older answer arrives last and must not bring the proposals back.
    await act(async () => {
      releaseOlder(list(a, b));
    });
    expect(screen.getAllByText(/Accepted by Acme Owner/)).toHaveLength(2);
    expect(screen.getByText("Nothing waiting for review.")).toBeInTheDocument();
  });

  it("keeps the Dismissed and Removed notes with the repository they were made in", async () => {
    const other = rule({ id: "r9", scope_key: "github:github.com:9", scope_label: "acme-corp/other", text: "other repo rule" });
    vi.mocked(listMemoryRules).mockResolvedValue(list(rule({ id: "ra", text: "first repo rule" }), other));
    vi.mocked(rejectMemoryRule).mockResolvedValue({ rule: rule({ id: "ra" }), applies_from: "next_ticket" });
    renderPage();

    await userEvent.click(await screen.findByRole("button", { name: "Dismiss" }));
    expect(await screen.findByText("Dismissed")).toBeInTheDocument();
    await userEvent.selectOptions(screen.getByTestId("memory-repository-select"), "github:github.com:9");
    expect(await screen.findByText("other repo rule")).toBeInTheDocument();
    expect(screen.queryByText("Dismissed")).toBeNull();
  });

  describe("a refusal stays with the repository and the action that caused it", () => {
    const other = () =>
      rule({ id: "rb", scope_key: "github:github.com:9", scope_label: "acme-corp/other", text: "other repo rule" });

    it("does not show repository A's refusal above repository B's rules", async () => {
      vi.mocked(listMemoryRules).mockResolvedValue(list(proposed(), other()));
      vi.mocked(acceptMemoryRule).mockRejectedValue(refusal(403, "memory_rules.write_access_required"));
      renderPage();

      await userEvent.click(await screen.findByRole("button", { name: "Accept" }));
      expect(await screen.findByText(/Only someone who can merge to acme-corp\/acme-bot/)).toBeInTheDocument();
      await userEvent.selectOptions(screen.getByTestId("memory-repository-select"), "github:github.com:9");
      expect(await screen.findByText("other repo rule")).toBeInTheDocument();
      expect(screen.queryByTestId("memory-rule-refusal")).toBeNull();
    });

    it("keeps a refusal whose rule left the list with its own repository only", async () => {
      vi.mocked(listMemoryRules)
        .mockResolvedValueOnce(list(proposed(), other()))
        .mockResolvedValue(list({ ...proposed(), state: "rejected", version: 5 }, other()));
      vi.mocked(acceptMemoryRule).mockRejectedValue(refusal(409, "memory_rules.stale_version"));
      renderPage();

      await userEvent.click(await screen.findByRole("button", { name: "Accept" }));
      expect(await screen.findByTestId("memory-rule-refusal")).toHaveTextContent("This rule changed. The list was refreshed.");
      await userEvent.selectOptions(screen.getByTestId("memory-repository-select"), "github:github.com:9");
      await screen.findByText("other repo rule");
      expect(screen.queryByTestId("memory-rule-refusal")).toBeNull();
      await userEvent.selectOptions(screen.getByTestId("memory-repository-select"), "github:github.com:4401");
      expect(await screen.findByTestId("memory-rule-refusal")).toBeInTheDocument();
    });
  });

  describe("a reload that fails after an action stays with the action", () => {
    const unavailable = () => new MemoryRuleRefusal(503, "memory_rules.unavailable", "try again", null);

    it("shows only the card refusal when the decision and the reload both fail with 503", async () => {
      vi.mocked(listMemoryRules).mockResolvedValueOnce(list(proposed())).mockRejectedValue(unavailable());
      vi.mocked(acceptMemoryRule).mockRejectedValue(refusal(503, "memory_rules.code_host_unavailable"));
      renderPage();

      await userEvent.click(await screen.findByRole("button", { name: "Accept" }));
      expect(await screen.findByText("The code host could not be reached. Nothing changed.")).toBeInTheDocument();
      expect(screen.queryByTestId("memory-rules-error")).toBeNull();
      expect(screen.getAllByRole("alert")).toHaveLength(1);
      expect(screen.getByRole("button", { name: "Accept" })).toBeEnabled();
    });

    it("does not claim the list was refreshed when the stale version reload fails", async () => {
      vi.mocked(listMemoryRules).mockResolvedValueOnce(list(proposed())).mockRejectedValue(unavailable());
      vi.mocked(acceptMemoryRule).mockRejectedValue(refusal(409, "memory_rules.stale_version"));
      renderPage();

      await userEvent.click(await screen.findByRole("button", { name: "Accept" }));
      expect(await screen.findByText("This rule changed. The list could not be refreshed.")).toBeInTheDocument();
      expect(screen.queryByText(/The list was refreshed/)).toBeNull();
      expect(screen.queryByTestId("memory-rules-error")).toBeNull();
      expect(screen.getByText("a proposed rule")).toBeInTheDocument();
    });

    it("says a saved change could not be refreshed, on the card", async () => {
      vi.mocked(listMemoryRules).mockResolvedValueOnce(list(proposed())).mockRejectedValue(unavailable());
      vi.mocked(acceptMemoryRule).mockResolvedValue({ rule: accepted(), applies_from: "next_ticket" });
      renderPage();

      await userEvent.click(await screen.findByRole("button", { name: "Accept" }));
      expect(await screen.findByText("The change was saved, but the list could not be refreshed.")).toBeInTheDocument();
      expect(screen.queryByTestId("memory-rules-error")).toBeNull();
    });

    it("judges the refresh by the newest reload when an older one resolves last", async () => {
      const a = rule({ id: "ra", text: "rule a", version: 1 });
      const b = rule({ id: "rb", text: "rule b", version: 1 });
      let releaseOlder!: (v: MemoryRules) => void;
      const older = new Promise<MemoryRules>((resolve) => (releaseOlder = resolve));
      vi.mocked(listMemoryRules)
        .mockResolvedValueOnce(list(a, b))
        .mockReturnValueOnce(older) // the reload after rule a's refused accept
        .mockRejectedValue(unavailable()); // the newer reload, after rule b's accept
      vi.mocked(acceptMemoryRule)
        .mockRejectedValueOnce(refusal(409, "memory_rules.stale_version"))
        .mockResolvedValueOnce({ rule: { ...b, state: "accepted" as const }, applies_from: "next_ticket" });
      renderPage();

      const cards = await screen.findAllByTestId("memory-rule");
      await userEvent.click(within(cards[0]).getByRole("button", { name: "Accept" }));
      await userEvent.click(within(screen.getAllByTestId("memory-rule")[1]).getByRole("button", { name: "Accept" }));
      expect(await screen.findByText("The change was saved, but the list could not be refreshed.")).toBeInTheDocument();
      // The older read answers last, successfully; the screen still shows the first list.
      await act(async () => {
        releaseOlder(list(a, b));
      });
      expect(await screen.findByText("This rule changed. The list could not be refreshed.")).toBeInTheDocument();
      expect(screen.queryByText("This rule changed. The list was refreshed.")).toBeNull();
    });

    it("still reports a failure of the first load on the page", async () => {
      vi.mocked(listMemoryRules).mockRejectedValue(unavailable());
      renderPage();
      expect(await screen.findByTestId("memory-rules-error")).toHaveTextContent("try again");
    });
  });

  it("409 invalid transition reloads without leaving the page in a busy state", async () => {
    vi.mocked(listMemoryRules)
      .mockResolvedValueOnce(list(proposed()))
      .mockResolvedValue(list({ ...proposed(), state: "rejected" }));
    vi.mocked(acceptMemoryRule).mockRejectedValue(refusal(409, "memory_rules.invalid_transition", "not allowed"));
    renderPage();

    await userEvent.click(await screen.findByRole("button", { name: "Accept" }));
    expect(await screen.findByText("Nothing waiting for review.")).toBeInTheDocument();
  });
});

describe("WiredMemoryPage Facts (#5800)", () => {
  it("renders the existing facts panel for the selected agent", async () => {
    vi.mocked(listMemoryRules).mockResolvedValue(list(PROPOSED));
    vi.mocked(listMemory).mockResolvedValue([
      {
        index: 0,
        version: 3,
        content: "deploy is a git push",
        provenance: { learned_from_session_id: "sess-1", source_trace_ids: ["trace-a"], recorded_at: "" },
      },
    ]);
    renderPage();

    await userEvent.click(await screen.findByRole("button", { name: "Facts" }));
    expect(screen.getByRole("button", { name: "Facts" })).toHaveAttribute("aria-pressed", "true");
    expect(await screen.findByTestId("agent-memory")).toBeInTheDocument();
    expect(await screen.findByText("deploy is a git push")).toBeInTheDocument();
    expect(listMemory).toHaveBeenCalledWith("ag-1");
    // The rules description belongs to Learned rules only.
    expect(screen.queryByText(/What this factory has learned on/)).toBeNull();
    expect(screen.getByRole("button", { name: "Edit" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Delete" })).toBeEnabled();
  });
});

describe("page helpers", () => {
  it("parses the author, the pull request number and the UTC time", () => {
    expect(authorOf("github:6601:octocat")).toBe("octocat");
    expect(authorOf("octocat")).toBeNull();
    expect(authorOf("github:6601:")).toBeNull();
    expect(authorOf("gitlab:7:someone")).toBeNull();
    expect(authorOf("a:b:c:d")).toBeNull();
    expect(pullRequestNumberOf("https://github.com/o/r/pull/12#discussion_r5")).toBe("12");
    expect(pullRequestNumberOf("https://gitlab.test/o/r/-/merge_requests/7")).toBe("7");
    expect(pullRequestNumberOf("https://example.test/x")).toBeNull();
    expect(pullRequestNumberOf(null)).toBeNull();
    expect(utcWhen("2026-11-18T23:59:59-05:00")).toBe("on 2026-11-19 04:59 UTC");
    expect(utcWhen(null)).toBe("at an unknown time");
  });
});
