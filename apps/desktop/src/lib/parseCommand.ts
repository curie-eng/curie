// Typed text -> a manifest invocation, or a stated reason why not.
//
// This is what lets the console accept typing without becoming a shell. The
// app's rule is that a value a user types must never be able to BECOME a
// command: nothing is concatenated into a string and executed, and there is no
// `shell: true` anywhere. So the console does not run text at all. It parses
// text into `{ action, positionals, flags }` -- an action that must name a
// command the manifest declares, and flags that must be flags that command
// declares -- and hands that to the same IPC call every button in the app uses.
// The main process then resolves argv independently and rejects anything it does
// not recognise, so a parser bug here fails closed rather than executing.
//
// What that buys, and what it costs, are both worth being explicit about:
//
//   - There is no `|`, no `>`, no `&&`, no `$(...)`, no globbing and no
//     environment expansion. Those are shell features and this is not a shell.
//     A typed `|` is a parse error, not a pipe.
//   - Quotes ARE handled, because an argument with a space in it is ordinary
//     (`local message "deploy the thing"`) and the alternative is making the
//     console unable to express something every button can.

import { commandsById, fieldKind, type Command, type ManifestArg } from "./manifest";

export interface ParsedCommand {
  readonly ok: true;
  readonly cmd: Command;
  readonly positionals: readonly string[];
  readonly flags: Readonly<Record<string, string | boolean>>;
  readonly json: boolean;
  /** Positionals the command requires and the text did not supply. Parsing still
   *  succeeds -- the console shows them as a prompt to finish rather than an
   *  error, the way the form does. */
  readonly missing: readonly string[];
}

export interface ParseError {
  readonly ok: false;
  readonly error: string;
  /** A command id to suggest, when the text nearly named one. */
  readonly suggestion?: string;
}

export type ParseResult = ParsedCommand | ParseError;

/** Shell metacharacters. Present only to be refused with an explanation: a
 *  console that silently dropped a `>` would look like it had redirected. */
const SHELL_ONLY = /[|&;><`$(){}[\]*?]/;

/**
 * Split on whitespace, honouring single and double quotes.
 *
 * Returns `null` for an unterminated quote rather than guessing where the
 * argument ended -- the operator is mid-word, and the console says so instead of
 * running half a value.
 */
export function tokenize(text: string): string[] | null {
  const out: string[] = [];
  let cur = "";
  let quote: '"' | "'" | null = null;
  let any = false;

  for (const ch of text) {
    if (quote) {
      if (ch === quote) quote = null;
      else cur += ch;
      continue;
    }
    if (ch === '"' || ch === "'") {
      quote = ch;
      any = true;
      continue;
    }
    if (/\s/.test(ch)) {
      if (cur || any) out.push(cur);
      cur = "";
      any = false;
      continue;
    }
    cur += ch;
  }
  if (quote) return null;
  if (cur || any) out.push(cur);
  return out;
}

/** The deepest command whose path prefixes these words, and what is left over. */
function matchCommand(words: readonly string[]): { cmd: Command; rest: string[] } | null {
  // Longest match first, so `local observability runs` beats a hypothetical
  // `local observability`.
  for (let take = Math.min(words.length, 4); take >= 1; take--) {
    const cmd = commandsById.get(words.slice(0, take).join("."));
    if (cmd) return { cmd, rest: words.slice(take) };
  }
  return null;
}

/** Nearest command by a cheap prefix/substring score, for "did you mean". */
function suggest(words: readonly string[]): string | undefined {
  const typed = words.join(" ");
  if (!typed) return undefined;
  let best: { id: string; score: number } | undefined;
  for (const cmd of commandsById.values()) {
    const spaced = cmd.path.join(" ");
    let score = 0;
    if (spaced.startsWith(typed)) score = 100 + (100 - spaced.length);
    else if (spaced.includes(words[0])) score = 50;
    else if (cmd.name.startsWith(words[words.length - 1] ?? "")) score = 25;
    if (score && (!best || score > best.score)) best = { id: spaced, score };
  }
  return best?.id;
}

export function parseCommand(text: string): ParseResult {
  const trimmed = text.trim();
  if (!trimmed) return { ok: false, error: "Type a command." };

  if (SHELL_ONLY.test(trimmed)) {
    return {
      ok: false,
      error:
        "This console runs curie commands, not a shell — no pipes, redirects, globs or substitution. Copy the command out to a terminal if you need those.",
    };
  }

  const words = tokenize(trimmed);
  if (!words) return { ok: false, error: "Unterminated quote." };

  // A leading `curie` is what everyone types and what every preview in the app
  // shows, so accept it and ignore it rather than making it an error.
  const rest0 = words[0] === "curie" ? words.slice(1) : words;
  if (!rest0.length) return { ok: false, error: "Type a command after `curie`." };

  const matched = matchCommand(rest0);
  if (!matched) {
    const guess = suggest(rest0);
    return {
      ok: false,
      error: `No command called \`${rest0.join(" ")}\`.`,
      suggestion: guess,
    };
  }

  const { cmd, rest } = matched;
  const declared = new Map<string, ManifestArg>();
  for (const f of cmd.flags) declared.set(f.long!, f);

  const positionals: string[] = [];
  const flags: Record<string, string | boolean> = {};
  let json = false;

  for (let i = 0; i < rest.length; i++) {
    const word = rest[i];

    if (!word.startsWith("--")) {
      positionals.push(word);
      continue;
    }

    // `--json` is carried on the invocation rather than as a flag, matching how
    // every other surface in the app requests it.
    const eq = word.indexOf("=");
    const name = (eq === -1 ? word.slice(2) : word.slice(2, eq)).trim();
    const inlineValue = eq === -1 ? undefined : word.slice(eq + 1);

    if (name === "json") {
      json = true;
      continue;
    }

    const spec = declared.get(name);
    if (!spec) {
      const near = [...declared.keys()].find((k) => k.startsWith(name) || name.startsWith(k));
      return {
        ok: false,
        error: `\`curie ${cmd.path.join(" ")}\` has no \`--${name}\`.${
          near ? ` Did you mean \`--${near}\`?` : ""
        }`,
      };
    }

    // A boolean flag takes no value; anything else consumes the next word. Not
    // consuming it would silently turn a value into a positional, which is how a
    // console quietly runs a different command than the one that was typed.
    if (fieldKind(spec) === "boolean") {
      flags[name] = inlineValue === undefined ? true : inlineValue !== "false";
      continue;
    }
    const value = inlineValue ?? rest[++i];
    if (value === undefined) return { ok: false, error: `\`--${name}\` needs a value.` };
    flags[name] = value;
  }

  if (positionals.length > cmd.positionals.length) {
    return {
      ok: false,
      error: `\`curie ${cmd.path.join(" ")}\` takes ${cmd.positionals.length} argument${
        cmd.positionals.length === 1 ? "" : "s"
      }, and ${positionals.length} were given.`,
    };
  }

  const missing = cmd.positionals
    .map((spec, i) => (spec.required && !positionals[i]?.trim() ? spec.id : null))
    .filter((x): x is string => !!x);

  return { ok: true, cmd, positionals, flags, json, missing };
}

/**
 * Completions for the word being typed.
 *
 * Command paths first, then the current command's own flags, because that is the
 * order the two become relevant: you name a command, then qualify it.
 */
export function complete(text: string): string[] {
  const words = tokenize(text) ?? [];
  const noCurie = words[0] === "curie" ? words.slice(1) : words;
  const trailingSpace = /\s$/.test(text);
  const partial = trailingSpace ? "" : (noCurie[noCurie.length - 1] ?? "");
  const before = trailingSpace ? noCurie : noCurie.slice(0, -1);

  // Once a command is named, complete its flags rather than more command names.
  const matched = matchCommand(before.length ? before : noCurie);
  if (matched && (before.length || trailingSpace)) {
    if (partial.startsWith("--") || partial === "") {
      const want = partial.replace(/^--/, "");
      return matched.cmd.flags
        .map((f) => `--${f.long!}`)
        .filter((f) => f.slice(2).startsWith(want))
        .sort();
    }
  }

  const prefix = [...before, partial].join(" ").trim();
  return [...commandsById.values()]
    .map((c) => c.path.join(" "))
    .filter((p) => p.startsWith(prefix) && p !== prefix)
    .sort()
    .slice(0, 12);
}
