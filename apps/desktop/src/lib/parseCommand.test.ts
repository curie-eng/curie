// The console's parser.
//
// This is the one place in the app where a command comes from free text rather
// than from a control, so it is the one place a typo could become a different
// command than the one intended. Every test here is about that: the parse either
// produces exactly what was typed, or refuses and says why. It never guesses.
//
// The main process validates argv again on the other side of the IPC boundary,
// so a bug here fails closed. That is a backstop, not a reason to be loose.

import { describe, expect, it } from "vitest";

import { complete, parseCommand, tokenize } from "./parseCommand";

/** Narrow to the success shape, failing with the parser's own message. */
function ok(text: string) {
  const r = parseCommand(text);
  if (!r.ok) throw new Error(`expected a parse, got: ${r.error}`);
  return r;
}

function err(text: string): string {
  const r = parseCommand(text);
  if (r.ok) throw new Error(`expected a refusal, got action ${r.cmd.id}`);
  return r.error;
}

describe("tokenizing", () => {
  it("splits on whitespace", () => {
    expect(tokenize("local up --minimal")).toEqual(["local", "up", "--minimal"]);
  });

  it("keeps a quoted argument whole, which is what makes a message typable", () => {
    expect(tokenize(`local message "deploy the thing"`)).toEqual([
      "local",
      "message",
      "deploy the thing",
    ]);
    expect(tokenize(`local message 'two words'`)).toEqual(["local", "message", "two words"]);
  });

  it("keeps an empty quoted argument, which is a real value", () => {
    expect(tokenize(`local message ""`)).toEqual(["local", "message", ""]);
  });

  it("refuses an unterminated quote rather than guessing where it ended", () => {
    expect(tokenize(`local message "half a`)).toBeNull();
    expect(err(`local message "half a`)).toMatch(/unterminated quote/i);
  });
});

describe("naming a command", () => {
  it("parses a two-word command", () => {
    expect(ok("local up").cmd.id).toBe("local.up");
  });

  it("accepts the leading `curie` everyone types", () => {
    expect(ok("curie local up").cmd.id).toBe("local.up");
  });

  it("prefers the longest matching path", () => {
    // `local observability` is a group with subcommands; the deeper one wins.
    expect(ok("local observability runs").cmd.id).toBe("local.observability.runs");
  });

  it("refuses an unknown command and suggests the nearest", () => {
    const r = parseCommand("local uppp");
    expect(r.ok).toBe(false);
    if (r.ok) return;
    expect(r.error).toMatch(/no command called/i);
    expect(r.suggestion).toBeTruthy();
  });

  it("refuses a command group with no subcommand", () => {
    // `local` alone is not runnable; only its leaves are.
    expect(err("local")).toMatch(/no command called/i);
  });
});

describe("flags", () => {
  it("takes a boolean flag with no value", () => {
    expect(ok("local up --minimal").flags).toEqual({ minimal: true });
  });

  it("takes a valued flag, consuming the next word", () => {
    expect(ok("local up --model sonnet").flags).toEqual({ model: "sonnet" });
  });

  it("takes --flag=value", () => {
    expect(ok("local up --model=sonnet").flags).toEqual({ model: "sonnet" });
  });

  it("does not let a flag's value fall through into a positional", () => {
    // The failure this prevents: `--api-url X <agent>` parsing X as the agent
    // and running against the wrong one.
    const r = ok("local memory --api-url http://x billing-bot");
    expect(r.flags).toEqual({ "api-url": "http://x" });
    expect(r.positionals).toEqual(["billing-bot"]);
  });

  it("refuses a flag the command does not declare", () => {
    expect(err("local up --nope")).toMatch(/has no `--nope`/);
  });

  it("suggests a near-miss flag", () => {
    expect(err("local up --minima")).toMatch(/--minimal/);
  });

  it("refuses a valued flag with nothing after it", () => {
    expect(err("local up --model")).toMatch(/needs a value/i);
  });

  it("carries --json on the invocation, not as a flag", () => {
    const r = ok("local status --json");
    expect(r.json).toBe(true);
    expect(r.flags).toEqual({});
  });
});

describe("positionals", () => {
  it("collects them in order", () => {
    expect(ok("local memory billing-bot").positionals).toEqual(["billing-bot"]);
  });

  it("reports a required one that is missing, without failing the parse", () => {
    // The console shows this as "finish the command", the way the form does,
    // rather than as a typo.
    const r = ok("local memory");
    expect(r.missing).toEqual(["agent"]);
  });

  it("refuses more positionals than the command takes", () => {
    expect(err("local memory a b c")).toMatch(/takes 1 argument/);
  });
});

describe("it is not a shell, and says so", () => {
  // Each of these is a shell feature. Accepting any of them silently would be
  // the app's central invariant broken: a typed value must never be able to
  // become a command.
  for (const text of [
    "local status | grep x",
    "local status > out.txt",
    "local status && local down",
    "local status; local down",
    "local deploy --agent $(whoami)",
    "local deploy --plugin-dir *",
    "local status `id`",
  ]) {
    it(`refuses ${JSON.stringify(text)}`, () => {
      expect(err(text)).toMatch(/not a shell/i);
    });
  }

  it("still allows the punctuation real arguments contain", () => {
    // A URL, a path and a hyphenated name are not shell syntax.
    const r = ok("local memory billing-bot --api-url http://localhost:8000/api");
    expect(r.positionals).toEqual(["billing-bot"]);
    expect(r.flags).toEqual({ "api-url": "http://localhost:8000/api" });
  });
});

describe("completion", () => {
  it("completes a command path from a prefix", () => {
    const hits = complete("local o");
    expect(hits.some((h) => h.startsWith("local observability"))).toBe(true);
  });

  it("completes flags once a command is named", () => {
    const hits = complete("local up --");
    expect(hits).toContain("--minimal");
    expect(hits.every((h) => h.startsWith("--"))).toBe(true);
  });

  it("never suggests the exact text back", () => {
    expect(complete("local up")).not.toContain("local up");
  });
});
