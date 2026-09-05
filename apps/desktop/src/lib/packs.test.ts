import { describe, expect, it } from "vitest";

import {
  DEFAULT_MAX_BYTES,
  EMPTY_PACKS,
  GREETING_PHRASES,
  PACK_IDS,
  PACK_KINDS,
  byteSize,
  caption,
  coerceSetting,
  crc32,
  deelongate,
  enabledPacks,
  inertPacks,
  isInert,
  matchGreeting,
  matchHelp,
  matchesBare,
  normalise,
  packIssues,
  parsePacks,
  pick,
  proposeFromBundle,
  sampleLoad,
  sampleTip,
  type BehaviorPacks,
  type Setting,
} from "./packs";

const setting = (over: Partial<Setting> = {}): Setting => ({
  key: "verbosity",
  label: "",
  kind: "str",
  default: "",
  help: "",
  choices: [],
  applies_live: true,
  ...over,
});

const packs = (over: Partial<BehaviorPacks>): BehaviorPacks => ({ ...EMPTY_PACKS, ...over });

describe("parsePacks is total", () => {
  // BehaviorPacks.from_config never raises: a corrupt blob on an agent row must
  // not brick the agent. An editor that threw on the same blob could not open an
  // agent the platform runs fine.
  it.each([
    ["null", null],
    ["undefined", undefined],
    ["a string", "nope"],
    ["a number", 7],
    ["an array", [1, 2]],
    ["an empty object", {}],
    ["nested garbage", { load: 5, greeting: { phrases: "hi", reply: 3 } }],
  ])("reads %s as all-off", (_label, raw) => {
    expect(parsePacks(raw)).toEqual(EMPTY_PACKS);
  });

  it("keeps the fields it recognises and drops non-strings from lists", () => {
    const parsed = parsePacks({
      load: { enabled: true, lines: ["one", 2, null, "three"] },
      nav: { enabled: true, hub_label: "Help", hub_command: "hub" },
      unknown_future_pack: { enabled: true },
    });
    expect(parsed.load).toEqual({ enabled: true, lines: ["one", "three"] });
    expect(parsed.nav).toEqual({ enabled: true, hub_label: "Help", hub_command: "hub" });
  });

  it("defaults applies_live to true, matching SettingConfig", () => {
    const parsed = parsePacks({ settings: { enabled: true, settings: [{ key: "k" }] } });
    expect(parsed.settings.settings[0]).toEqual(setting({ key: "k" }));
  });

  it("round-trips what it produces", () => {
    const once = parsePacks({ tips: { enabled: true, tips: ["a"] } });
    expect(parsePacks(JSON.parse(JSON.stringify(once)))).toEqual(once);
  });
});

describe("normalise mirrors _normalize", () => {
  it.each([
    ["Hi!", "hi"],
    ["  HEY   there  ", "hey there"],
    ["Good Morning, team.", "good morning team"],
    ["café", "cafe"],
    ["hi 👋", "hi"],
    ["what's up?", "what s up"],
    ["...", ""],
    ["ROUTE-66", "route 66"],
  ])("%s -> %s", (input, expected) => {
    expect(normalise(input)).toBe(expected);
  });
});

describe("deelongate mirrors _deelongate", () => {
  it.each([
    ["hiiii", "hi"],
    ["sooo", "so"],
    ["hii", "hii"], // two is not a run of three
    ["hello", "hello"],
  ])("%s -> %s", (input, expected) => {
    expect(deelongate(input)).toBe(expected);
  });
});

describe("matchesBare mirrors _matches_bare", () => {
  const greetings = ["hi", "hey", "good morning"];

  it.each(["hi", "Hi!", "hey", "hey there", "hey there team", "good morning everyone", "hiiii"])(
    "%s is bare",
    (text) => {
      expect(matchesBare(greetings, text)).toBe(true);
    },
  );

  it.each([
    "hi show me the report", // a real request wearing a greeting
    "morning", // not a declared phrase
    "", // nothing at all
    "   ",
    "say hi", // the phrase must start the utterance
    "good", // a prefix of a phrase is not the phrase
  ])("%s is not bare", (text) => {
    expect(matchesBare(greetings, text)).toBe(false);
  });

  it("ignores a phrase that normalises to nothing rather than matching everything", () => {
    expect(matchesBare(["!!!"], "anything")).toBe(false);
    expect(matchesBare(["!!!"], "")).toBe(false);
  });

  it("requires the whole phrase, not a shared first word", () => {
    expect(matchesBare(["good morning"], "good evening")).toBe(false);
  });
});

describe("the reply short circuit is the platform's", () => {
  // match_greeting returns None when the reply is empty, BEFORE looking at the
  // phrases. This is the trap the UI exists to name: a pack with phrases and no
  // reply is enabled and silently inert.
  it("does not fire without a reply, however many phrases are declared", () => {
    const p = packs({ greeting: { enabled: true, phrases: ["hi"], reply: "" } });
    expect(matchGreeting(p, "hi")).toBeNull();
    expect(matchesBare(p.greeting.phrases, "hi")).toBe(true);
  });

  it("does not fire when disabled", () => {
    expect(matchGreeting(packs({ greeting: { enabled: false, phrases: ["hi"], reply: "yo" } }), "hi")).toBeNull();
  });

  it("fires when both halves are present", () => {
    expect(matchGreeting(packs({ greeting: { enabled: true, phrases: ["hi"], reply: "yo" } }), "hi")).toBe("yo");
  });

  it("applies the same rules to help", () => {
    const p = packs({ help: { enabled: true, phrases: ["what can you do"], reply: "lots" } });
    expect(matchHelp(p, "what can you do?")).toBe("lots");
    expect(matchHelp(p, "what can you do about the outage")).toBeNull();
  });
});

describe("the sampler mirrors _pick", () => {
  it("returns the first item for an empty seed", () => {
    expect(pick(["a", "b", "c"], "")).toBe("a");
  });

  it("is empty for an empty list", () => {
    expect(pick([], "seed")).toBe("");
  });

  it("is deterministic for a seed", () => {
    expect(pick(["a", "b", "c"], "1712.0001")).toBe(pick(["a", "b", "c"], "1712.0001"));
  });

  it("salts load and tips apart, so one seed varies both independently", () => {
    // The whole reason _pick takes a salt: the kernel seeds both off the same
    // conversation id, and without the salt they would rotate in lockstep.
    const p = packs({
      load: { enabled: true, lines: ["l0", "l1", "l2", "l3"] },
      tips: { enabled: true, tips: ["t0", "t1", "t2", "t3"] },
    });
    const seeds = ["a", "b", "c", "d", "e", "f", "g", "h"];
    const pairs = seeds.map((s) => [sampleLoad(p, s), sampleTip(p, s)] as const);
    // Indices must not agree for every seed.
    const lockstep = pairs.every(([l, t]) => l?.slice(1) === t?.slice(1));
    expect(lockstep).toBe(false);
  });

  it("crc32 matches known values", () => {
    // Anchors the table implementation against the standard, since the whole
    // point is landing on the same element as zlib.crc32.
    expect(crc32("")).toBe(0);
    expect(crc32("a")).toBe(0xe8b7be43);
    expect(crc32("hello world")).toBe(0x0d4a1185);
  });

  it("returns null for a disabled or empty pack", () => {
    expect(sampleLoad(EMPTY_PACKS, "s")).toBeNull();
    expect(sampleLoad(packs({ load: { enabled: true, lines: [] } }), "s")).toBeNull();
  });
});

describe("caption mirrors kernel._shimmer", () => {
  const withBoth = packs({
    load: { enabled: true, lines: ["Working on it"] },
    tips: { enabled: true, tips: ["I can rank leaks by $"] },
  });

  it("composes load and tip", () => {
    expect(caption(withBoth, "s", "Thinking...")).toBe("Working on it\n\nTip: I can rank leaks by $");
  });

  it("uses the load line alone", () => {
    expect(caption(packs({ load: { enabled: true, lines: ["Working"] } }), "s", "Thinking...")).toBe("Working");
  });

  it("prefixes a lone tip", () => {
    expect(caption(packs({ tips: { enabled: true, tips: ["I rank leaks"] } }), "s", "Thinking...")).toBe(
      "Tip: I rank leaks",
    );
  });

  it("falls back to the deployment's status text when no pack yields a line", () => {
    // This is why an enabled-but-empty pack looks broken: the platform default
    // shows instead, and nothing says the pack was skipped.
    expect(caption(packs({ load: { enabled: true, lines: [] } }), "s", "Thinking...")).toBe("Thinking...");
  });

  it("is null when the operator has blanked the status text", () => {
    expect(caption(EMPTY_PACKS, "s", "")).toBeNull();
  });
});

describe("coerceSetting mirrors coerce_setting", () => {
  it.each([
    ["int", "3", "3"],
    ["int", " 42 ", "42"],
    ["bool", "on", "true"],
    ["bool", "YES", "true"],
    ["bool", "0", "false"],
    ["choice", "b", "b"],
    ["str", "anything", "anything"],
  ])("%s accepts %s", (kind, raw, expected) => {
    const res = coerceSetting(setting({ kind, choices: ["a", "b"] }), raw);
    expect(res).toEqual({ ok: true, value: expected });
  });

  it.each([
    ["int", "", "must be a whole number"],
    ["int", "two", "must be a whole number"],
    ["int", "1.5", "must be a whole number"],
    ["int", "0", "must be 1 or more"],
    ["int", "-4", "must be 1 or more"],
    ["bool", "maybe", "use on or off"],
    ["choice", "z", "choose one of: a, b"],
    ["str", "  ", "cannot be empty"],
  ])("%s rejects %s", (kind, raw, error) => {
    expect(coerceSetting(setting({ kind, choices: ["a", "b"] }), raw)).toEqual({ ok: false, error });
  });
});

describe("byteSize measures what the API caps", () => {
  it("counts compact JSON of every field", () => {
    // The API measures json.dumps(model_dump(), separators=(",",":")) in UTF-8
    // bytes, so all six packs are present even when all-off.
    expect(byteSize(EMPTY_PACKS)).toBe(JSON.stringify(EMPTY_PACKS).length);
    expect(byteSize(EMPTY_PACKS)).toBeLessThan(DEFAULT_MAX_BYTES);
  });

  it("counts multi-byte characters as bytes, not characters", () => {
    const p = packs({ load: { enabled: true, lines: ["café ☕"] } });
    expect(byteSize(p)).toBeGreaterThan(JSON.stringify(p).length);
  });

  it("flags packs over the cap as an error", () => {
    const huge = packs({ load: { enabled: true, lines: ["x".repeat(DEFAULT_MAX_BYTES + 1)] } });
    expect(packIssues(huge).some((i) => i.level === "error" && i.message.includes("cap"))).toBe(true);
  });
});

describe("packIssues names the failures that are otherwise silent", () => {
  it("says nothing about all-off packs", () => {
    expect(packIssues(EMPTY_PACKS)).toEqual([]);
  });

  it("flags a greeting with phrases but no reply as an error", () => {
    const issues = packIssues(packs({ greeting: { enabled: true, phrases: ["hi"], reply: "" } }));
    expect(issues).toEqual([
      { pack: "greeting", level: "error", message: expect.stringContaining("never fires") },
    ]);
  });

  it("flags a greeting with a reply but no phrases", () => {
    const issues = packIssues(packs({ greeting: { enabled: true, phrases: [], reply: "yo" } }));
    expect(issues.some((i) => i.level === "error" && i.message.includes("no trigger phrases"))).toBe(true);
  });

  it("warns rather than errors on an empty load pack, because the platform just falls back", () => {
    const issues = packIssues(packs({ load: { enabled: true, lines: [] } }));
    expect(issues).toHaveLength(1);
    expect(issues[0].level).toBe("warn");
  });

  it("catches phrases that are the same phrase once normalised", () => {
    const issues = packIssues(
      packs({ greeting: { enabled: true, phrases: ["Hi!", "hi", "hiiii"], reply: "yo" } }),
    );
    const dupes = issues.filter((i) => i.message.includes("same phrase"));
    expect(dupes).toHaveLength(2);
  });

  it("catches a phrase that normalises to nothing", () => {
    const issues = packIssues(packs({ greeting: { enabled: true, phrases: ["!!!"], reply: "yo" } }));
    expect(issues.some((i) => i.level === "error" && i.message.includes("normalises to nothing"))).toBe(true);
  });

  it("warns about a filler-only phrase", () => {
    const issues = packIssues(packs({ greeting: { enabled: true, phrases: ["team"], reply: "yo" } }));
    expect(issues.some((i) => i.message.includes("filler"))).toBe(true);
  });

  it("warns when help declares a phrase the greeting already owns", () => {
    // The kernel evaluates `match_greeting(...) or match_help(...)`, so the help
    // reply is unreachable for a shared phrase.
    const issues = packIssues(
      packs({
        greeting: { enabled: true, phrases: ["hi", "help"], reply: "yo" },
        help: { enabled: true, phrases: ["help"], reply: "here is help" },
      }),
    );
    expect(issues.some((i) => i.pack === "help" && i.message.includes("greeting pack"))).toBe(true);
  });

  it("does not warn about a clash when only one of the two can fire", () => {
    const issues = packIssues(
      packs({
        greeting: { enabled: true, phrases: ["help"], reply: "" },
        help: { enabled: true, phrases: ["help"], reply: "here is help" },
      }),
    );
    expect(issues.some((i) => i.message.includes("greeting pack"))).toBe(false);
  });

  it("flags duplicate, keyless, unknown-kind and choiceless settings", () => {
    const issues = packIssues(
      packs({
        settings: {
          enabled: true,
          settings: [
            setting({ key: "a" }),
            setting({ key: "a" }),
            setting({ key: "" }),
            setting({ key: "b", kind: "colour" }),
            setting({ key: "c", kind: "choice", choices: [] }),
          ],
        },
      }),
    );
    const errors = issues.filter((i) => i.level === "error").map((i) => i.message);
    expect(errors.some((m) => m.includes('share the key "a"'))).toBe(true);
    expect(errors.some((m) => m.includes("has no key"))).toBe(true);
    expect(errors.some((m) => m.includes('kind "colour"'))).toBe(true);
    expect(errors.some((m) => m.includes("choice with no choices"))).toBe(true);
  });

  it("warns, not errors, on a default that fails its own validation", () => {
    // The API stores any default string, so calling this invalid would make the
    // app stricter than the platform.
    const issues = packIssues(
      packs({ settings: { enabled: true, settings: [setting({ key: "n", kind: "int", default: "zero" })] } }),
    );
    const issue = issues.find((i) => i.message.includes("fails its own validation"));
    expect(issue?.level).toBe("warn");
  });

  it("says an enabled settings pack does nothing yet", () => {
    const issues = packIssues(packs({ settings: { enabled: true, settings: [setting()] } }));
    expect(issues.some((i) => i.level === "info" && i.message.includes("no runtime reads them"))).toBe(true);
  });

  it("flags a nav pack missing either half", () => {
    const issues = packIssues(packs({ nav: { enabled: true, hub_label: "Help", hub_command: "" } }));
    expect(issues.some((i) => i.level === "error" && i.message.includes("leads nowhere") === false)).toBe(true);
    expect(issues.filter((i) => i.pack === "nav")).toHaveLength(1);
  });
});

describe("isInert", () => {
  it("is false for every disabled pack", () => {
    PACK_IDS.forEach((id) => expect(isInert(EMPTY_PACKS, id)).toBe(false));
  });

  it.each([
    ["load", packs({ load: { enabled: true, lines: [] } })],
    ["tips", packs({ tips: { enabled: true, tips: [] } })],
    ["greeting", packs({ greeting: { enabled: true, phrases: ["hi"], reply: " " } })],
    ["help", packs({ help: { enabled: true, phrases: [], reply: "x" } })],
    ["settings", packs({ settings: { enabled: true, settings: [] } })],
    ["nav", packs({ nav: { enabled: true, hub_label: "Help", hub_command: "" } })],
  ])("spots an enabled but unusable %s pack", (id, p) => {
    expect(isInert(p, id as never)).toBe(true);
  });

  it("is false for a fully configured pack", () => {
    expect(isInert(packs({ nav: { enabled: true, hub_label: "Help", hub_command: "hub" } }), "nav")).toBe(false);
  });
});

describe("enabledPacks", () => {
  it("lists them in pack order", () => {
    const p = packs({
      nav: { enabled: true, hub_label: "H", hub_command: "h" },
      load: { enabled: true, lines: ["a"] },
    });
    expect(enabledPacks(p)).toEqual(["load", "nav"]);
  });
});

describe("proposeFromBundle", () => {
  const seed = {
    name: "sre-bot",
    description: "Triages alerts and ranks cost leaks.",
    starterPrompts: ["Rank our cost leaks by dollars", "What broke last night?"],
  };

  it("turns starter prompts into tips, because that is what they are", () => {
    expect(proposeFromBundle(seed).tips).toEqual({ enabled: true, tips: seed.starterPrompts });
  });

  it("writes a greeting from the bundle's own facts", () => {
    const greeting = proposeFromBundle(seed).greeting;
    expect(greeting.enabled).toBe(true);
    expect(greeting.reply).toContain("sre-bot");
    expect(greeting.reply).toContain("Triages alerts");
    expect(greeting.reply).toContain("Rank our cost leaks");
    expect(greeting.phrases).toEqual(GREETING_PHRASES);
  });

  it("proposes nothing it cannot fill honestly", () => {
    // No load lines: inventing what an agent says it is doing would be putting
    // words in its mouth.
    const proposed = proposeFromBundle({});
    expect(proposed.load).toEqual(EMPTY_PACKS.load);
    expect(proposed.tips).toEqual(EMPTY_PACKS.tips);
    expect(proposed.settings).toEqual(EMPTY_PACKS.settings);
    expect(proposed.nav).toEqual(EMPTY_PACKS.nav);
  });

  it("keeps packs the author already configured", () => {
    const current = packs({ nav: { enabled: true, hub_label: "Home", hub_command: "hub" } });
    expect(proposeFromBundle(seed, current).nav).toEqual(current.nav);
  });

  it("proposes packs that pass their own review and actually fire", () => {
    // The proposal must not be something packIssues then complains about.
    const proposed = proposeFromBundle(seed);
    expect(packIssues(proposed).filter((i) => i.level === "error")).toEqual([]);
    expect(matchGreeting(proposed, "hey there team")).toBe(proposed.greeting.reply);
    expect(matchHelp(proposed, "what can you do?")).toBe(proposed.help.reply);
    expect(matchGreeting(proposed, "hi, rank our cost leaks")).toBeNull();
  });
});

describe("PACK_KINDS", () => {
  it("covers every pack exactly once, in wire order", () => {
    expect(PACK_KINDS.map((k) => k.id)).toEqual([...PACK_IDS]);
  });

  it("marks settings as the one pack no runtime reads", () => {
    // Grounded in the repo: resolve_settings and coerce_setting have no call
    // site outside behaviorpacks.py, and the doc says the runtime is deferred.
    expect(PACK_KINDS.filter((k) => !k.live).map((k) => k.id)).toEqual(["settings"]);
  });
});

describe("inertPacks", () => {
  it("is empty when nothing is on", () => {
    expect(inertPacks(EMPTY_PACKS)).toEqual([]);
  });

  it("is empty when every enabled pack is usable", () => {
    const p = packs({
      load: { enabled: true, lines: ["working"] },
      nav: { enabled: true, hub_label: "Help", hub_command: "hub" },
    });
    expect(inertPacks(p)).toEqual([]);
  });

  it("lists only the enabled packs that cannot fire, in pack order", () => {
    const p = packs({
      load: { enabled: true, lines: [] },
      tips: { enabled: true, tips: ["a tip"] },
      greeting: { enabled: true, phrases: ["hi"], reply: "" },
      nav: { enabled: false, hub_label: "", hub_command: "" },
    });
    expect(inertPacks(p)).toEqual(["load", "greeting"]);
  });
});
