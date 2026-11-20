import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import {
  ApiError,
  acceptMemoryRule,
  listMemoryRules,
  MemoryRuleRefusal,
  MemoryRuleShapeError,
  rejectMemoryRule,
  removeMemoryRule,
} from "./client";

// Handoff proof (#5800): the answers of the real routes, recorded by the API
// tests (tests/vectors/memory-rules-responses.json, which they fail on when it
// drifts, and which the CLI tests decode too), go through the console's own
// client; a body that differs from the documented shape is refused by name.
const VECTOR = JSON.parse(
  readFileSync(join(process.cwd(), "../../tests/vectors/memory-rules-responses.json"), "utf8"),
) as { list: { rules: Record<string, unknown>[] }; accept: { rule: Record<string, unknown>; applies_from: string } };

const AGENT = "11111111-1111-1111-1111-111111111111";
const RULE = "22222222-2222-2222-2222-222222222222";

function answer(status: number, body: unknown) {
  return vi.fn().mockImplementation(async () =>
    new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } }),
  );
}

beforeEach(() => {
  vi.stubGlobal("fetch", answer(200, VECTOR.list));
});
afterEach(() => {
  vi.unstubAllGlobals();
});

const without = (rule: Record<string, unknown>, field: string) => {
  const { [field]: _omit, ...rest } = rule;
  return rest;
};

describe("listMemoryRules against the recorded list answer", () => {
  it("returns the recorded rules, every state, unchanged", async () => {
    const got = await listMemoryRules(AGENT);
    expect(got.rules.map((r) => r.state)).toEqual(["proposed", "accepted", "rejected", "removed"]);
    expect(got.in_force_limit).toBe(16);
    const [url, init] = vi.mocked(fetch).mock.calls[0];
    expect(url).toBe(`/api/agents/${AGENT}/memory/rules`);
    expect(init).toMatchObject({ credentials: "same-origin" });
  });

  it("refuses a rule that has no version, naming the field", async () => {
    const rules = [without(VECTOR.list.rules[0], "version")];
    vi.stubGlobal("fetch", answer(200, { ...VECTOR.list, rules }));
    await expect(listMemoryRules(AGENT)).rejects.toThrow(MemoryRuleShapeError);
    await expect(listMemoryRules(AGENT)).rejects.toThrow(/rules\[0\]\.version/);
  });

  it.each([
    ["a state the API never sends", { state: "pending" }, /rules\[0\]\.state/],
    ["a text that is not a string", { text: 5 }, /rules\[0\]\.text/],
    ["an accepted_by without a principal", { accepted_by: { display_name: "x" } }, /rules\[0\]\.accepted_by/],
  ])("refuses %s", async (_n, patch, at) => {
    const rules = [{ ...VECTOR.list.rules[0], ...patch }];
    vi.stubGlobal("fetch", answer(200, { ...VECTOR.list, rules }));
    await expect(listMemoryRules(AGENT)).rejects.toThrow(at);
  });

  it("refuses a body without a rules list", async () => {
    vi.stubGlobal("fetch", answer(200, { scopes: [], in_force_limit: 16 }));
    await expect(listMemoryRules(AGENT)).rejects.toThrow(/`rules`/);
  });
});

describe("accept, reject and remove against the recorded accept answer", () => {
  const actions = [
    ["accept", acceptMemoryRule],
    ["reject", rejectMemoryRule],
    ["remove", removeMemoryRule],
  ] as const;

  it.each(actions)("%s posts exactly the expected version to the rule's own route", async (name, fn) => {
    vi.stubGlobal("fetch", answer(200, VECTOR.accept));
    const change = await fn(AGENT, RULE, 4);
    expect(change.applies_from).toBe("next_ticket");
    expect(change.rule.version).toBe(2);
    const [url, init] = vi.mocked(fetch).mock.calls[0];
    expect(url).toBe(`/api/agents/${AGENT}/memory/rules/${RULE}/${name}`);
    expect(init?.method).toBe("POST");
    expect(JSON.parse(init?.body as string)).toEqual({ expected_version: 4 });
  });

  it("refuses an answer whose rule has no version", async () => {
    vi.stubGlobal("fetch", answer(200, { ...VECTOR.accept, rule: without(VECTOR.accept.rule, "version") }));
    await expect(acceptMemoryRule(AGENT, RULE, 1)).rejects.toThrow(/rule\.version/);
  });

  it("keeps the code, the message and the limit of a coded refusal", async () => {
    vi.stubGlobal(
      "fetch",
      answer(409, {
        detail: { code: "memory_rules.scope_full", message: "The scope is full.", fix: "Remove a rule.", limit: 16 },
      }),
    );
    const error = await acceptMemoryRule(AGENT, RULE, 1).catch((e) => e);
    expect(error).toBeInstanceOf(MemoryRuleRefusal);
    expect(error).toMatchObject({ status: 409, code: "memory_rules.scope_full", message: "The scope is full.", limit: 16 });
  });

  it("reports a plain-string refusal (a console origin or credential refusal) as an ApiError", async () => {
    vi.stubGlobal("fetch", answer(403, { detail: "forbidden origin" }));
    const error = await acceptMemoryRule(AGENT, RULE, 1).catch((e) => e);
    expect(error).not.toBeInstanceOf(MemoryRuleRefusal);
    expect(error).toMatchObject({ status: 403, message: "forbidden origin" });
  });
});

// --- the requests the client sends, replayed by the API tests ------------------------

const REQUESTS = join(process.cwd(), "../../tests/vectors/memory-rules-console-requests.json");
const RESPONSES = join(process.cwd(), "../../tests/vectors/memory-rules-console-responses.json");
const REQUESTS_NOTE =
  "The requests apps/ui/src/api/client.ts sends for the learned rule routes, recorded by " +
  "apps/ui/src/api/memoryRules.client.test.ts, which fails when they drift, and replayed against the " +
  "registered routes by apps/api/tests/test_memory_rule_routes.py (console_client). Ids are placeholders. " +
  "Refresh: CURIE_WRITE_MEMORY_RULES_VECTOR=1 pnpm exec vitest run src/api/memoryRules.client.test.ts";

describe("the requests the client sends", () => {
  it("match the recorded vector the API tests replay against the routes", async () => {
    const requests = [];
    for (const [operation, call] of [
      ["list", () => listMemoryRules(AGENT)],
      ["accept", () => acceptMemoryRule(AGENT, RULE, 1)],
      ["reject", () => rejectMemoryRule(AGENT, RULE, 1)],
      ["remove", () => removeMemoryRule(AGENT, RULE, 1)],
    ] as const) {
      vi.stubGlobal("fetch", answer(200, operation === "list" ? VECTOR.list : VECTOR.accept));
      await call();
      const [url, init] = vi.mocked(fetch).mock.calls[0];
      requests.push({
        operation,
        method: init?.method ?? "GET",
        // The dev proxy and the ingress strip the `/api` prefix before the API sees it.
        path: String(url).replace(/^\/api/, ""),
        body: init?.body ? JSON.parse(init.body as string) : null,
      });
    }
    const document = { _comment: REQUESTS_NOTE, agent_id: AGENT, rule_id: RULE, requests };
    if (process.env.CURIE_WRITE_MEMORY_RULES_VECTOR === "1") {
      writeFileSync(REQUESTS, JSON.stringify(document, null, 2) + "\n");
    }
    expect(JSON.parse(readFileSync(REQUESTS, "utf8"))).toEqual(document);
  });
});

// --- the answers of the routes, read back through the client -------------------------

type Recorded = { status: number; body: { rule: { state: string; version: number }; detail: { code?: string; message: string } } };
const answers = () => JSON.parse(readFileSync(RESPONSES, "utf8")) as Record<string, Recorded>;

describe("the recorded decision answers of each route", () => {
  it.each([
    ["accept", acceptMemoryRule, "accepted"],
    ["reject", rejectMemoryRule, "rejected"],
    ["remove", removeMemoryRule, "removed"],
  ] as const)("%s answers a rule in its own state", async (name, fn, state) => {
    const recorded = answers()[name];
    vi.stubGlobal("fetch", answer(recorded.status, recorded.body));
    const change = await fn(AGENT, RULE, 1);
    expect(change.rule.state).toBe(state);
    expect(change.rule.version).toBe(recorded.body.rule.version);
    // The three routes answer three different rules: none reuses another's payload.
    const others = ["accept", "reject", "remove"].filter((other) => other !== name);
    for (const other of others) expect(answers()[other].body.rule.state).not.toBe(state);
  });

  it.each([
    ["refusal_stale_version", 409, "memory_rules.stale_version"],
    ["refusal_write_access_required", 403, "memory_rules.write_access_required"],
    ["refusal_code_host_unavailable", 503, "memory_rules.code_host_unavailable"],
  ])("%s keeps its status and code", async (name, status, code) => {
    const recorded = answers()[name];
    vi.stubGlobal("fetch", answer(recorded.status, recorded.body));
    const error = await acceptMemoryRule(AGENT, RULE, 1).catch((e) => e);
    expect(error).toBeInstanceOf(MemoryRuleRefusal);
    expect(error).toMatchObject({ status, code, message: recorded.body.detail.message });
  });

  it("a refusal recorded without its code is not a coded refusal", async () => {
    const { code: _code, ...detail } = answers().refusal_stale_version.body.detail;
    vi.stubGlobal("fetch", answer(409, { detail }));
    const error = await acceptMemoryRule(AGENT, RULE, 1).catch((e) => e);
    expect(error).not.toBeInstanceOf(MemoryRuleRefusal);
    expect(error).toBeInstanceOf(ApiError);
    expect(error.message).toBe(detail.message);
  });
});
