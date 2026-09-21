// Behavior packs: the per-agent, opt-in Slack layer, mirrored for the desktop app.
//
// A pack is declarative JSON on an agent's row (docs/behavior-packs.md): trigger
// phrases, "working..." load lines, capability tips, a canned reply, a hub
// button. The platform owns the matcher and the sampler; only the data varies.
// Six packs exist -- load, tips, greeting, help, settings, nav.
//
// Two things make this worth a module of its own rather than a form.
//
// First, **the CLI cannot do this at all.** There is no `curie ... behavior-packs`
// verb; the only surface is `GET|PUT /agents/{id}/behavior-packs`. So unlike every
// other view in this app, this one is not catching up to the CLI -- it is the
// first place outside the web console where a pack can be authored.
//
// Second, **a pack can be enabled and still be inert**, and nothing tells you.
// `match_greeting` returns None when the reply is empty, so a pack with ten
// phrases and no reply is silently off. `sample_load` returns None on an empty
// list, so the platform's generic caption is used and the pack looks broken
// rather than unconfigured. The matcher also normalises before comparing, so two
// phrases an author thinks are different can be the same phrase. Those are the
// failures this module names, which is why the matcher below is a faithful mirror
// of `curie_worker.behaviorpacks` rather than an approximation: a preview that
// disagrees with the worker is worse than no preview.
//
// Kept pure and framework-free so it can be tested without a window.

/** Wire shape of `BehaviorPacksConfig` (apps/api/src/curie_api/schemas.py).
 *  snake_case, because this is what crosses the API, not a UI type. */
export interface LoadPack {
  readonly enabled: boolean;
  readonly lines: readonly string[];
}

export interface TipsPack {
  readonly enabled: boolean;
  readonly tips: readonly string[];
}

/** Greeting and help are the same shape: the niceties battery's two halves. */
export interface PhrasePack {
  readonly enabled: boolean;
  readonly phrases: readonly string[];
  readonly reply: string;
}

export type SettingKind = "str" | "int" | "bool" | "choice";

export interface Setting {
  readonly key: string;
  readonly label: string;
  readonly kind: string;
  readonly default: string;
  readonly help: string;
  readonly choices: readonly string[];
  readonly applies_live: boolean;
}

export interface SettingsPack {
  readonly enabled: boolean;
  readonly settings: readonly Setting[];
}

export interface NavPack {
  readonly enabled: boolean;
  readonly hub_label: string;
  readonly hub_command: string;
}

export interface BehaviorPacks {
  readonly load: LoadPack;
  readonly tips: TipsPack;
  readonly greeting: PhrasePack;
  readonly help: PhrasePack;
  readonly settings: SettingsPack;
  readonly nav: NavPack;
}

export type PackId = keyof BehaviorPacks;

/** Field order mirrors the pydantic models, so `byteSize` below measures the
 *  same bytes the API's cap measures. */
export const EMPTY_PACKS: BehaviorPacks = {
  load: { enabled: false, lines: [] },
  tips: { enabled: false, tips: [] },
  greeting: { enabled: false, phrases: [], reply: "" },
  help: { enabled: false, phrases: [], reply: "" },
  settings: { enabled: false, settings: [] },
  nav: { enabled: false, hub_label: "", hub_command: "" },
};

export const PACK_IDS: readonly PackId[] = ["load", "tips", "greeting", "help", "settings", "nav"];

export interface PackKind {
  readonly id: PackId;
  readonly title: string;
  /** What it does, in the author's terms. */
  readonly what: string;
  /** Where it lands in Slack, so an author knows what they are configuring. */
  readonly surface: string;
  /** False when the platform stores and validates the pack but no runtime reads
   *  it yet. Only the settings pack is in that state, and the UI must say so
   *  rather than implying the knobs work. */
  readonly live: boolean;
}

/**
 * The six packs. `live` is not decoration: `docs/behavior-packs.md` calls the
 * settings pack "schema only in this PR; the durable override store and edit UI
 * are a deferred runtime", and `resolve_settings`/`coerce_setting` have no call
 * site anywhere outside their own module. Declaring settings is therefore
 * legitimate and inert, and an author who is not told that will conclude the
 * feature is broken.
 */
export const PACK_KINDS: readonly PackKind[] = [
  {
    id: "load",
    title: "Load lines",
    what: 'Rotating "working..." lines describing what the agent is doing right now.',
    surface: "Slack's assistant-thread status caption, while a turn runs.",
    live: true,
  },
  {
    id: "tips",
    title: "Tips",
    what: 'Rotating capability tips advertising what the agent can do ("I can rank leaks by $").',
    surface: 'The same caption, appended as "Tip: ...".',
    live: true,
  },
  {
    id: "greeting",
    title: "Greeting",
    what: 'A canned reply to a bare greeting ("hi", "hey there team"). Never calls the model.',
    surface: "A normal Slack reply, sent without a turn.",
    live: true,
  },
  {
    id: "help",
    title: "Help",
    what: 'A canned reply to a bare "what can you do". Never calls the model.',
    surface: "A normal Slack reply, sent without a turn.",
    live: true,
  },
  {
    id: "settings",
    title: "Settings",
    what: "A declared allowlist of user-editable runtime knobs.",
    surface: "Nothing yet: the platform validates these, but no runtime reads them.",
    live: false,
  },
  {
    id: "nav",
    title: "Hub button",
    what: "A way back to the agent's home screen, so a structured reply is never a dead end.",
    surface: "Appended to a reply's buttons when none already links there.",
    live: true,
  },
];

export const PACK_KIND_BY_ID: Readonly<Record<PackId, PackKind>> = Object.fromEntries(
  PACK_KINDS.map((k) => [k.id, k]),
) as Record<PackId, PackKind>;

export const SETTING_KINDS: readonly SettingKind[] = ["str", "int", "bool", "choice"];

// --- parsing -----------------------------------------------------------------
//
// `BehaviorPacks.from_config` is TOTAL by contract: a missing, non-mapping or
// malformed blob becomes all-off rather than an error, because a corrupt blob on
// an agent row must never brick that agent's turns. This mirrors that, for the
// same reason in reverse: a pack editor that throws on a blob the platform
// tolerates would refuse to open an agent the platform runs fine.

function strList(v: unknown): string[] {
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === "string") : [];
}

function bool(v: unknown, fallback = false): boolean {
  return typeof v === "boolean" ? v : fallback;
}

function str(v: unknown): string {
  return typeof v === "string" ? v : "";
}

function obj(v: unknown): Record<string, unknown> {
  return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : {};
}

function parseSetting(raw: unknown): Setting {
  const o = obj(raw);
  return {
    key: str(o.key),
    label: str(o.label),
    kind: typeof o.kind === "string" ? o.kind : "str",
    default: str(o.default),
    help: str(o.help),
    choices: strList(o.choices),
    applies_live: bool(o.applies_live, true),
  };
}

/** Total: never throws. Anything unrecognised reads as the all-off default. */
export function parsePacks(raw: unknown): BehaviorPacks {
  const o = obj(raw);
  const load = obj(o.load);
  const tips = obj(o.tips);
  const greeting = obj(o.greeting);
  const help = obj(o.help);
  const settings = obj(o.settings);
  const nav = obj(o.nav);
  return {
    load: { enabled: bool(load.enabled), lines: strList(load.lines) },
    tips: { enabled: bool(tips.enabled), tips: strList(tips.tips) },
    greeting: {
      enabled: bool(greeting.enabled),
      phrases: strList(greeting.phrases),
      reply: str(greeting.reply),
    },
    help: { enabled: bool(help.enabled), phrases: strList(help.phrases), reply: str(help.reply) },
    settings: {
      enabled: bool(settings.enabled),
      settings: Array.isArray(settings.settings) ? settings.settings.map(parseSetting) : [],
    },
    nav: {
      enabled: bool(nav.enabled),
      hub_label: str(nav.hub_label),
      hub_command: str(nav.hub_command),
    },
  };
}

// --- the matcher, mirrored ---------------------------------------------------

/** Words that may trail a greeting without making it a real request. Mirrors
 *  `behaviorpacks._FILLER`; platform-owned and deliberately small. */
export const FILLER: ReadonlySet<string> = new Set([
  "there",
  "team",
  "all",
  "everyone",
  "folks",
  "yall",
  "guys",
  "peeps",
  "bot",
]);

/** Casefold, strip accents/punctuation/emoji, collapse whitespace.
 *  Mirrors `behaviorpacks._normalize`. */
export function normalise(text: string): string {
  const folded = text.normalize("NFKD").replace(/\p{M}+/gu, "");
  return folded
    .toLowerCase()
    .replace(/[^a-z0-9\s]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

/** Collapse 3+ repeated characters to one: "hiiii" -> "hi".
 *  Mirrors `behaviorpacks._deelongate`. */
export function deelongate(norm: string): string {
  return norm.replace(/(.)\1{2,}/g, "$1");
}

/**
 * True if `text` is a *bare* utterance of one of `phrases` -- the phrase alone,
 * or the phrase followed only by filler. A phrase glued to a real request ("hi
 * show me the report") is not bare, and falls through to the model.
 *
 * Mirrors `behaviorpacks._matches_bare`, including the prefix rule (the phrase
 * must start the utterance) and the second pass over elongation-collapsed
 * tokens.
 */
export function matchesBare(phrases: readonly string[], text: string): boolean {
  const norm = normalise(text);
  if (!norm) return false;
  const tokens = norm.split(" ");
  const squeezed = deelongate(norm).split(" ");
  for (const rawPhrase of phrases) {
    const phrase = normalise(rawPhrase);
    if (!phrase) continue;
    const ptoks = phrase.split(" ");
    for (const candidate of [tokens, squeezed]) {
      const head = candidate.slice(0, ptoks.length);
      if (head.length !== ptoks.length) continue;
      if (head.every((t, i) => t === ptoks[i]) && candidate.slice(ptoks.length).every((t) => FILLER.has(t)))
        return true;
    }
  }
  return false;
}

/** The canned reply if `text` is a bare greeting, else null. Note the
 *  reply-empty short circuit: that is the platform's, and it is what makes a
 *  phrase-only pack inert. Mirrors `match_greeting`. */
export function matchGreeting(packs: BehaviorPacks, text: string): string | null {
  const pack = packs.greeting;
  if (!pack.enabled || !pack.reply) return null;
  return matchesBare(pack.phrases, text) ? pack.reply : null;
}

/** Mirrors `match_help`. */
export function matchHelp(packs: BehaviorPacks, text: string): string | null {
  const pack = packs.help;
  if (!pack.enabled || !pack.reply) return null;
  return matchesBare(pack.phrases, text) ? pack.reply : null;
}

// --- the sampler, mirrored ---------------------------------------------------

const CRC_TABLE: Int32Array = (() => {
  const table = new Int32Array(256);
  for (let i = 0; i < 256; i += 1) {
    let c = i;
    for (let k = 0; k < 8; k += 1) c = c & 1 ? 0xedb88320 ^ (c >>> 1) : c >>> 1;
    table[i] = c;
  }
  return table;
})();

/** zlib.crc32 over the UTF-8 bytes, so `pick` lands on the same element the
 *  worker lands on for a given seed. */
export function crc32(s: string): number {
  const bytes = new TextEncoder().encode(s);
  let c = -1;
  for (const b of bytes) c = (c >>> 8) ^ CRC_TABLE[(c ^ b) & 0xff];
  return (c ^ -1) >>> 0;
}

/** Deterministically pick one item, rotated by `seed`. Empty seed -> the first
 *  item. Mirrors `behaviorpacks._pick`. */
export function pick(seq: readonly string[], seed: string, salt = ""): string {
  if (!seq.length) return "";
  if (!seed) return seq[0];
  return seq[crc32(`${salt}${seed}`) % seq.length];
}

/** Mirrors `sample_load`: null when the pack is off or empty. */
export function sampleLoad(packs: BehaviorPacks, seed: string): string | null {
  if (!packs.load.enabled) return null;
  return pick(packs.load.lines, seed, "w:") || null;
}

/** Mirrors `sample_tip`. Salted separately so both vary off one message ts. */
export function sampleTip(packs: BehaviorPacks, seed: string): string | null {
  if (!packs.tips.enabled) return null;
  return pick(packs.tips.tips, seed, "t:") || null;
}

/**
 * The caption the worker actually sets, for a given seed. Mirrors the
 * composition in `kernel.py::_shimmer`, including the fall back to the
 * platform's generic status text when neither pack yields a line -- which is the
 * detail that makes an empty-but-enabled pack look broken instead of unset.
 *
 * `fallback` is the deployment's `status_text`; an operator who blanks it wants
 * no caption at all, and an empty caption would read as a clear.
 */
export function caption(packs: BehaviorPacks, seed: string, fallback: string): string | null {
  const load = sampleLoad(packs, seed);
  const tip = sampleTip(packs, seed);
  if (load && tip) return `${load}\n\nTip: ${tip}`;
  if (load) return load;
  if (tip) return `Tip: ${tip}`;
  return fallback || null;
}

// --- setting coercion, mirrored ---------------------------------------------

const TRUTHY: ReadonlySet<string> = new Set(["1", "true", "yes", "on"]);
const FALSY: ReadonlySet<string> = new Set(["0", "false", "no", "off"]);

export type Coerced = { readonly ok: true; readonly value: string } | { readonly ok: false; readonly error: string };

/** Validate and normalise a raw string for `setting`. Mirrors `coerce_setting`,
 *  returning the failure rather than throwing. */
export function coerceSetting(setting: Setting, raw: string): Coerced {
  const value = (raw || "").trim();
  if (setting.kind === "int") {
    if (!/^[+-]?\d+$/.test(value)) return { ok: false, error: "must be a whole number" };
    const n = Number.parseInt(value, 10);
    if (n < 1) return { ok: false, error: "must be 1 or more" };
    return { ok: true, value: String(n) };
  }
  if (setting.kind === "bool") {
    const low = value.toLowerCase();
    if (TRUTHY.has(low)) return { ok: true, value: "true" };
    if (FALSY.has(low)) return { ok: true, value: "false" };
    return { ok: false, error: "use on or off" };
  }
  if (setting.kind === "choice") {
    if (!setting.choices.includes(value))
      return { ok: false, error: `choose one of: ${setting.choices.join(", ")}` };
    return { ok: true, value };
  }
  if (!value) return { ok: false, error: "cannot be empty" };
  return { ok: true, value };
}

// --- size cap ----------------------------------------------------------------

/** The API's default `behavior_packs_max_bytes`. Configurable server side, so a
 *  local check is advisory: it catches the mistake before the round trip, it does
 *  not replace the 413. */
export const DEFAULT_MAX_BYTES = 64 * 1024;

/** The serialized byte length the API measures: compact JSON of the whole
 *  config, all fields present, in model order. `EMPTY_PACKS`'s field order is
 *  the pydantic field order for exactly this reason. */
export function byteSize(packs: BehaviorPacks): number {
  return new TextEncoder().encode(JSON.stringify(packs)).length;
}

// --- authoring problems ------------------------------------------------------

export type IssueLevel = "error" | "warn" | "info";

export interface PackIssue {
  readonly pack: PackId;
  readonly level: IssueLevel;
  readonly message: string;
}

function phraseIssues(pack: PackId, phrases: readonly string[]): PackIssue[] {
  const out: PackIssue[] = [];
  const seen = new Map<string, string>();
  phrases.forEach((raw) => {
    const norm = normalise(raw);
    if (!norm) {
      out.push({
        pack,
        level: "error",
        message: `"${raw}" normalises to nothing, so it can never match.`,
      });
      return;
    }
    // The matcher compares both the normalised and the de-elongated tokens, so
    // two phrases that differ only in punctuation, case, accents or a run of
    // repeated letters are one phrase.
    const key = deelongate(norm);
    const prior = seen.get(key);
    if (prior !== undefined) {
      out.push({
        pack,
        level: "warn",
        message: `"${raw}" and "${prior}" are the same phrase once normalised ("${key}").`,
      });
      return;
    }
    seen.set(key, raw);
    if (norm.split(" ").every((t) => FILLER.has(t))) {
      out.push({
        pack,
        level: "warn",
        message: `"${raw}" is made only of filler words, so it matches far more than it looks like it does.`,
      });
    }
  });
  return out;
}

function phrasePackIssues(id: "greeting" | "help", pack: PhrasePack): PackIssue[] {
  if (!pack.enabled) return [];
  const out: PackIssue[] = [];
  // The platform's own short circuit: no reply means the matcher returns None
  // before it ever looks at the phrases.
  if (!pack.reply.trim())
    out.push({
      pack: id,
      level: "error",
      message: "Enabled with no reply, so it never fires: the matcher requires a reply.",
    });
  if (!pack.phrases.length)
    out.push({
      pack: id,
      level: "error",
      message: "Enabled with no trigger phrases, so nothing can match it.",
    });
  return [...out, ...phraseIssues(id, pack.phrases)];
}

function settingsIssues(pack: SettingsPack): PackIssue[] {
  if (!pack.enabled) return [];
  const out: PackIssue[] = [];
  if (!pack.settings.length)
    out.push({ pack: "settings", level: "warn", message: "Enabled with no settings declared." });
  const seen = new Set<string>();
  pack.settings.forEach((s, i) => {
    const where = s.key || `setting ${i + 1}`;
    if (!s.key.trim())
      out.push({ pack: "settings", level: "error", message: `${where} has no key.` });
    else if (seen.has(s.key))
      out.push({
        pack: "settings",
        level: "error",
        message: `Two settings share the key "${s.key}"; the later one wins and the earlier is unreachable.`,
      });
    seen.add(s.key);
    if (!SETTING_KINDS.includes(s.kind as SettingKind))
      out.push({
        pack: "settings",
        level: "error",
        message: `${where} has kind "${s.kind}", which is not one of ${SETTING_KINDS.join(", ")}.`,
      });
    if (s.kind === "choice" && !s.choices.length)
      out.push({
        pack: "settings",
        level: "error",
        message: `${where} is a choice with no choices.`,
      });
    if (s.default) {
      const coerced = coerceSetting(s, s.default);
      if (!coerced.ok)
        out.push({
          pack: "settings",
          level: "warn",
          message: `${where}'s default "${s.default}" fails its own validation: ${coerced.error}.`,
        });
    }
  });
  return out;
}

/**
 * Everything wrong with a set of packs, in the author's terms.
 *
 * `error` here means "the platform will store this and it will not do anything",
 * not "the API will reject it". That distinction is deliberate: every pack this
 * function flags is schema-valid, so refusing to save one would make this app
 * stricter than the platform it is a client of. The UI states the problem and
 * still lets the author save.
 */
export function packIssues(packs: BehaviorPacks): readonly PackIssue[] {
  const out: PackIssue[] = [];

  if (packs.load.enabled && !packs.load.lines.length)
    out.push({
      pack: "load",
      level: "warn",
      message: "Enabled with no lines, so the platform's generic caption is used instead.",
    });
  if (packs.tips.enabled && !packs.tips.tips.length)
    out.push({ pack: "tips", level: "warn", message: "Enabled with no tips, so no tip is shown." });

  out.push(...phrasePackIssues("greeting", packs.greeting));
  out.push(...phrasePackIssues("help", packs.help));
  out.push(...settingsIssues(packs.settings));

  if (packs.nav.enabled) {
    if (!packs.nav.hub_label.trim())
      out.push({ pack: "nav", level: "error", message: "Enabled with no button label." });
    if (!packs.nav.hub_command.trim())
      out.push({
        pack: "nav",
        level: "error",
        message: "Enabled with no hub command, so the button would lead nowhere.",
      });
  }

  // A greeting and a help pack that both claim the same phrase: the kernel tries
  // greeting first (`match_greeting(...) or match_help(...)`), so the help reply
  // is unreachable for that phrase.
  if (packs.greeting.enabled && packs.help.enabled && packs.greeting.reply && packs.help.reply) {
    const greetKeys = new Set(packs.greeting.phrases.map((p) => deelongate(normalise(p))).filter(Boolean));
    const clash = packs.help.phrases.filter((p) => greetKeys.has(deelongate(normalise(p))));
    if (clash.length)
      out.push({
        pack: "help",
        level: "warn",
        message: `${clash.map((c) => `"${c}"`).join(", ")} also trigger${clash.length === 1 ? "s" : ""} the greeting pack, which is tried first, so the help reply never sends for ${clash.length === 1 ? "it" : "them"}.`,
      });
  }

  if (packs.settings.enabled)
    out.push({
      pack: "settings",
      level: "info",
      message:
        "Settings are stored and validated, but no runtime reads them yet, so declaring them changes nothing today.",
    });

  const size = byteSize(packs);
  if (size > DEFAULT_MAX_BYTES)
    out.push({
      pack: "load",
      level: "error",
      message: `These packs are ${size} bytes, over the API's default ${DEFAULT_MAX_BYTES}-byte cap; the write will be rejected.`,
    });

  return out;
}

/** The packs that are switched on. */
export function enabledPacks(packs: BehaviorPacks): readonly PackId[] {
  return PACK_IDS.filter((id) => packs[id].enabled);
}

/** True when a pack is on but carries nothing the runtime can use, which is the
 *  state that reads as a platform bug rather than an unfinished config. */
export function isInert(packs: BehaviorPacks, id: PackId): boolean {
  const pack = packs[id];
  if (!pack.enabled) return false;
  switch (id) {
    case "load":
      return !packs.load.lines.length;
    case "tips":
      return !packs.tips.tips.length;
    case "greeting":
    case "help": {
      const p = packs[id];
      return !p.reply.trim() || !p.phrases.length;
    }
    case "settings":
      return !packs.settings.settings.length;
    case "nav":
      return !packs.nav.hub_label.trim() || !packs.nav.hub_command.trim();
  }
}

/** The packs that are on but carry nothing the runtime can use. What an
 *  inventory of agents needs: "this one is configured" and "this one only looks
 *  configured" are different answers. */
export function inertPacks(packs: BehaviorPacks): readonly PackId[] {
  return PACK_IDS.filter((id) => isInert(packs, id));
}

export function samePacks(a: BehaviorPacks, b: BehaviorPacks): boolean {
  return JSON.stringify(a) === JSON.stringify(b);
}

// --- proposing packs from the bundle ----------------------------------------
//
// This is what makes the Build screen the right home for an editor whose data
// lives on an agent row. A bundle already declares the facts a greeting, a help
// reply and a set of tips are made of: its name, its description, and its
// starter prompts, which are literally "things you can ask me". Proposing from
// those beats an empty form, and the author edits from there.

export interface PackSeed {
  readonly name?: string;
  readonly description?: string;
  readonly starterPrompts?: readonly string[];
}

export const GREETING_PHRASES: readonly string[] = [
  "hi",
  "hello",
  "hey",
  "yo",
  "sup",
  "good morning",
  "good afternoon",
  "good evening",
];

export const HELP_PHRASES: readonly string[] = [
  "help",
  "commands",
  "what can you do",
  "what do you do",
  "how do i use you",
];

/**
 * A first draft of the niceties packs, built from what the bundle already says.
 * Returns only the packs it can honestly fill: with no starter prompts there is
 * nothing to make tips out of, and inventing load lines would be putting words
 * in the agent's mouth.
 */
export function proposeFromBundle(seed: PackSeed, current: BehaviorPacks = EMPTY_PACKS): BehaviorPacks {
  const name = (seed.name ?? "").trim();
  const description = (seed.description ?? "").trim();
  const prompts = (seed.starterPrompts ?? []).map((p) => p.trim()).filter(Boolean);

  const who = name ? `I am ${name}.` : "";
  const does = description ? ` ${description}` : "";
  const asks = prompts.length ? `\n\nTry:\n${prompts.map((p) => `- ${p}`).join("\n")}` : "";

  const greetingReply = `${who}${does}${asks}`.trim();
  const helpReply = `${[who, does.trim()].filter(Boolean).join(" ")}${asks || "\n\nAsk me in your own words and I will do my best."}`.trim();

  return {
    ...current,
    tips: prompts.length ? { enabled: true, tips: prompts } : current.tips,
    greeting: greetingReply
      ? { enabled: true, phrases: [...GREETING_PHRASES], reply: greetingReply }
      : current.greeting,
    help: helpReply ? { enabled: true, phrases: [...HELP_PHRASES], reply: helpReply } : current.help,
  };
}
