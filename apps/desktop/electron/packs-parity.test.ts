// Differential test: the app's behavior-pack mirror against the real worker.
//
// `src/lib/packs.ts` reimplements `curie_worker.behaviorpacks` in TypeScript so
// the Build view can tell an author what their pack will actually do -- which
// phrases collide once normalised, which utterance fires a canned reply, which
// caption Slack will show. A mirror that is merely plausible is worse than no
// mirror: it would confidently show a preview the platform disagrees with.
//
// So this runs both implementations over the same corpus and compares. It is the
// same stance as `manifest.test.ts` (the rendered preview and the executed argv
// are two implementations that must agree) and it skips, loudly, when Python is
// not available -- the way `cli.integration.test.ts` skips without `curie`.

import { execFileSync } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { beforeAll, describe, expect, it } from "vitest";

import {
  EMPTY_PACKS,
  coerceSetting,
  deelongate,
  matchesBare,
  normalise,
  pick,
  sampleLoad,
  sampleTip,
  type BehaviorPacks,
  type Setting,
} from "../src/lib/packs";

const HERE = dirname(new URL(import.meta.url).pathname);
const WORKER = resolve(HERE, "..", "..", "..", "apps", "worker");

/**
 * `behaviorpacks.py` itself imports nothing but the standard library and pydantic,
 * so this runs `--no-project`: syncing the worker's whole environment to compare a
 * regex would put a heavy dependency install in the desktop CI job for no gain.
 */
const UV = ["run", "--no-project", "--with", "pydantic", "python"];

/**
 * Load the module by FILE PATH, rather than importing `curie_worker.behaviorpacks`.
 *
 * The package's `__init__.py` imports `binding`, which imports `aci_protocol`,
 * which pulls in the rest of the worker. So importing the module the normal way
 * needs the worker's entire environment even though the module under test needs
 * only pydantic -- which is what CI found, and what the probe below reported.
 * Loading the file directly keeps this test's dependencies equal to its subject's.
 *
 * The module must be registered in `sys.modules` BEFORE it executes: it uses
 * `from __future__ import annotations`, so pydantic resolves its forward
 * references by looking the module up by name, and a module that is not there yet
 * fails with "SettingsPack is not fully defined".
 */
const LOADER = `
import importlib.util, sys
_spec = importlib.util.spec_from_file_location("bp", "src/curie_worker/behaviorpacks.py")
bp = importlib.util.module_from_spec(_spec)
sys.modules["bp"] = bp
_spec.loader.exec_module(bp)
`;

/** Prints one JSON answer per case, for the whole corpus. */
const SCRIPT = `
import json, sys
${LOADER}

req = json.load(sys.stdin)
out = {
    "normalize": [bp._normalize(t) for t in req["normalize"]],
    "deelongate": [bp._deelongate(t) for t in req["deelongate"]],
    "matches_bare": [bp._matches_bare(p, t) for p, t in req["matches_bare"]],
    "pick": [bp._pick(seq, seed, salt) for seq, seed, salt in req["pick"]],
    "coerce": [],
    "sample": [],
}
for raw_setting, raw in req["coerce"]:
    try:
        out["coerce"].append({"ok": True, "value": bp.coerce_setting(bp.Setting(**raw_setting), raw)})
    except bp.SettingError as exc:
        out["coerce"].append({"ok": False, "error": str(exc)})
for blob, seed in req["sample"]:
    packs = bp.BehaviorPacks.from_config(blob)
    out["sample"].append([bp.sample_load(packs, seed), bp.sample_tip(packs, seed)])
json.dump(out, sys.stdout)
`;

/**
 * Why this suite may legitimately be skipped, or null to run it.
 *
 * The two failure modes must not be conflated. No `uv` on PATH is a property of
 * the machine and a fair skip. `uv` present but the module failing to import is a
 * property of the REPO -- the module moved, was renamed, or grew a dependency
 * beyond pydantic -- and skipping on that would silently retire the only check
 * that the mirror is still faithful. So that one throws.
 */
function probe(): string | null {
  if (!existsSync(WORKER)) return "apps/worker is not in this checkout";
  try {
    execFileSync("uv", ["--version"], { stdio: "pipe", timeout: 60_000 });
  } catch {
    return "uv is not on PATH";
  }
  try {
    execFileSync("uv", [...UV, "-c", LOADER], {
      cwd: WORKER,
      env: { ...process.env, PYTHONPATH: "src" },
      stdio: "pipe",
      timeout: 180_000,
    });
  } catch (err) {
    const detail = (err as { stderr?: Buffer }).stderr?.toString().trim().split("\n").slice(-3).join(" ");
    throw new Error(
      `uv is available but behaviorpacks.py would not load, so the ` +
        `behavior-pack mirror cannot be checked against it. If the module grew a ` +
        `dependency, add it to the --with list in UV above. ${detail ?? ""}`,
      { cause: err },
    );
  }
  return null;
}

const unavailable = probe();

// --- the corpus --------------------------------------------------------------
//
// Chosen to hit the edges that a reimplementation gets wrong: accent folding,
// emoji, punctuation-only input, the elongation pass, the filler tail, the
// phrase-must-be-a-prefix rule, and every branch of coerce_setting.

const TEXTS: readonly string[] = [
  "hi",
  "Hi!",
  "  HEY   there  ",
  "hey there team",
  "hey there boss",
  "good morning, everyone.",
  "hi show me the report",
  "hiiii",
  "hiii there",
  "sooo",
  "hii",
  "café",
  "CAFÉ",
  "naïve résumé",
  "hi 👋",
  "👋",
  "...",
  "!!!",
  "",
  "   ",
  "route-66",
  "what's up?",
  "WHAT CAN YOU DO",
  "what can you do about the outage",
  "help",
  "help me",
  "help team",
  "say hi",
  "good",
  "morning",
  "yall",
  "team",
  "\tmixed\n whitespace ",
  "ünïcödé bot",
  "a".repeat(200),
  "3 alerts fired",
  "1.5",
];

const PHRASE_SETS: readonly (readonly string[])[] = [
  ["hi", "hey", "good morning"],
  ["what can you do", "help", "commands"],
  ["!!!"],
  [""],
  [],
  ["team"],
  ["café"],
  ["hiiii"],
  ["good"],
];

const SEQS: readonly (readonly string[])[] = [[], ["only"], ["a", "b"], ["a", "b", "c", "d", "e"]];
const SEEDS: readonly string[] = ["", "1712345678.000100", "C099ABC", "café", "0", "thread-1", "👋"];
const SALTS: readonly string[] = ["", "w:", "t:"];

const KINDS: readonly string[] = ["str", "int", "bool", "choice", "colour"];
const RAWS: readonly string[] = [
  "",
  "   ",
  "3",
  " 42 ",
  "0",
  "-4",
  "1.5",
  "two",
  "on",
  "OFF",
  "yes",
  "0 ",
  "true",
  "maybe",
  "a",
  "z",
  "anything",
];

const setting = (kind: string): Setting => ({
  key: "k",
  label: "",
  kind,
  default: "",
  help: "",
  choices: ["a", "b"],
  applies_live: true,
});

const SAMPLE_PACKS: readonly BehaviorPacks[] = [
  EMPTY_PACKS,
  { ...EMPTY_PACKS, load: { enabled: true, lines: [] } },
  { ...EMPTY_PACKS, load: { enabled: true, lines: ["Working on it"] } },
  {
    ...EMPTY_PACKS,
    load: { enabled: true, lines: ["l0", "l1", "l2", "l3"] },
    tips: { enabled: true, tips: ["t0", "t1", "t2"] },
  },
  { ...EMPTY_PACKS, tips: { enabled: true, tips: ["only tip"] } },
];

const matchCases = PHRASE_SETS.flatMap((phrases) => TEXTS.map((text) => [phrases, text] as const));
const pickCases = SEQS.flatMap((seq) =>
  SEEDS.flatMap((seed) => SALTS.map((salt) => [seq, seed, salt] as const)),
);
const coerceCases = KINDS.flatMap((kind) => RAWS.map((raw) => [setting(kind), raw] as const));
const sampleCases = SAMPLE_PACKS.flatMap((p) => SEEDS.map((seed) => [p, seed] as const));

interface Answer {
  readonly normalize: readonly string[];
  readonly deelongate: readonly string[];
  readonly matches_bare: readonly boolean[];
  readonly pick: readonly string[];
  readonly coerce: readonly ({ ok: true; value: string } | { ok: false; error: string })[];
  readonly sample: readonly (readonly [string | null, string | null])[];
}

function ask(): Answer {
  const payload = JSON.stringify({
    normalize: TEXTS,
    deelongate: [...TEXTS, ...TEXTS.map(normalise)],
    matches_bare: matchCases,
    pick: pickCases,
    coerce: coerceCases,
    sample: sampleCases,
  });
  const out = execFileSync("uv", [...UV, "-c", SCRIPT], {
    cwd: WORKER,
    env: { ...process.env, PYTHONPATH: "src" },
    input: payload,
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
    timeout: 180_000,
  });
  return JSON.parse(out) as Answer;
}

const describeParity = unavailable ? describe.skip : describe;
if (unavailable) console.warn(`[packs-parity] skipped: ${unavailable}`);

describeParity("the app's pack mirror agrees with curie_worker.behaviorpacks", () => {
  // One subprocess for the whole corpus: the cost is the interpreter start, not
  // the cases, so there is no reason to be stingy with cases.
  //
  // In beforeAll rather than in the suite body, because `describe.skip` still
  // EXECUTES the body -- it only marks the tests skipped. Calling the subprocess
  // there made this file fail outright on a machine without `uv`, which is most
  // CI runners, so the skip was decorative.
  let py: Answer;
  beforeAll(() => {
    py = ask();
  }, 200_000);

  it("compares a corpus big enough to mean something", () => {
    expect(matchCases.length).toBeGreaterThan(300);
    expect(pickCases.length).toBeGreaterThan(80);
    expect(coerceCases.length).toBeGreaterThan(80);
    // Guards against a truncated answer silently comparing nothing.
    expect(py.matches_bare).toHaveLength(matchCases.length);
    expect(py.coerce).toHaveLength(coerceCases.length);
  });

  it("normalises identically", () => {
    expect(TEXTS.map(normalise)).toEqual([...py.normalize]);
  });

  it("de-elongates identically", () => {
    expect([...TEXTS, ...TEXTS.map(normalise)].map(deelongate)).toEqual([...py.deelongate]);
  });

  it("agrees on every bare-utterance decision", () => {
    const mine = matchCases.map(([phrases, text]) => matchesBare(phrases, text));
    // Reported as case -> verdict pairs so a failure names the input.
    const disagree = matchCases
      .map((c, i) => ({ phrases: c[0], text: c[1], mine: mine[i], worker: py.matches_bare[i] }))
      .filter((r) => r.mine !== r.worker);
    expect(disagree).toEqual([]);
  });

  it("does fire on some cases, so agreement is not agreement on 'never'", () => {
    expect(py.matches_bare.filter(Boolean).length).toBeGreaterThan(5);
  });

  it("picks the same element for every seed and salt", () => {
    const mine = pickCases.map(([seq, seed, salt]) => pick(seq, seed, salt));
    expect(mine).toEqual([...py.pick]);
  });

  it("coerces settings identically, successes and messages alike", () => {
    const mine = coerceCases.map(([s, raw]) => coerceSetting(s, raw));
    const disagree = coerceCases
      .map((c, i) => ({ kind: c[0].kind, raw: c[1], mine: mine[i], worker: py.coerce[i] }))
      .filter((r) => JSON.stringify(r.mine) !== JSON.stringify(r.worker));
    expect(disagree).toEqual([]);
  });

  it("samples the same load line and tip", () => {
    const mine = sampleCases.map(([p, seed]) => [sampleLoad(p, seed), sampleTip(p, seed)]);
    expect(mine).toEqual(py.sample.map((s) => [...s]));
  });

  it("parses the app's own wire shape without loss", () => {
    // These blobs went through BehaviorPacks.from_config on the Python side. If
    // the app emitted a field name the models do not know, from_config would have
    // quietly produced an all-off pack and every sample would come back null --
    // so a configured pack yielding a real line is what proves the wire shape.
    const configured = sampleCases
      .map(([p, seed], i) => ({ seed, load: py.sample[i][0], on: p.load.enabled && p.load.lines.length > 0 }))
      .filter((r) => r.on);
    expect(configured.length).toBeGreaterThan(0);
    expect(configured.filter((r) => !r.load)).toEqual([]);
  });

}, 200_000);
