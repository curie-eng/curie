// A real form for any command in the manifest.
//
// This is how the app keeps its promise not to be a worse experience than the
// CLI. Nothing here is written per command: the fields, their help text, their
// allowed values and their defaults all come from the same manifest `curie
// schema` prints, so all 80 commands are reachable and a command added to the
// CLI appears here without anyone building a screen for it.
//
// What the form adds on top of typing the command is the part a terminal cannot
// do: the arguments are discoverable instead of remembered, the values that
// repeat across commands are pre-filled from context, the exact command string
// is visible and copyable at all times, and the ones that destroy something ask
// first. What it deliberately does not do is hide the command -- the string
// under the form is the whole truth about what will run.

import { useCallback, useMemo, useState } from "react";

import { useApp, type Prefill } from "../bridge/app";
import { useRuns } from "../bridge/runs";
import { commandTitle } from "../lib/surfaces";
import { bridge } from "../bridge/bridge";
import { ACCENT, F, FONT, HUE, LINE, M, R, S, STATUS, T } from "../tokens";
import { Badge, Button, CopyButton, Field, Input, Mono, Notice, Select, Sheet, Textarea, Toggle } from "../primitives";
import {
  cwdFor,
  cwdReason,
  fieldKind,
  humanArg,
  runtimeDefault,
  defaultValue,
  renderCommand,
  NEEDS_TERMINAL,
  STICKY_FLAGS,
  type Command,
  type ManifestArg,
} from "../lib/manifest";

type Values = Record<string, string | boolean | undefined>;

/** Seed a form from the manifest's defaults plus whatever the app already knows
 *  (the open bundle, the last-used API URL). A field the operator has since
 *  edited is never overwritten -- `seed` runs only when the command changes. */
/**
 * The directory the command will run in.
 *
 * Every invocation this form launches carries `cwd: app.workspace?.path`, so the
 * bundle chosen in the sidebar decides where `skill up`, `skill check` and
 * `skill eval` do their work -- from any tab, including the palette. Nothing said
 * so anywhere, which is what made a global control look like decoration for the
 * Build tab, and made "the exact command that will run" less than exact: for a
 * skill-tier command the directory IS the argument.
 *
 * The fallback comes from the shell (`CURIE_WORKSPACE` or the home directory)
 * rather than being guessed here, because a directory this app prints and does
 * not actually use would be worse than printing none.
 */
function RunDirectory({ cmd }: { cmd: Command }) {
  const app = useApp();
  const ctx = {
    workspace: app.workspace?.path,
    repoRoot: app.env?.repoRoot,
    fallback: app.env?.defaultCwd,
  };
  const where = cwdFor(cmd, ctx);

  return (
    <div style={{ ...F.footnote, color: T.quaternary, marginBottom: 12, marginTop: -6 }}>
      {where === undefined ? (
        "Working directory not known yet."
      ) : (
        <>
          Runs in <Mono style={{ color: T.tertiary }}>{where}</Mono> — {cwdReason(where, ctx)}
        </>
      )}
    </div>
  );
}

function seedValues(
  cmd: Command,
  ctx: {
    workspacePath: string | null;
    sticky: Readonly<Record<string, string>>;
    prefill?: Prefill;
  },
): { positionals: string[]; flags: Values } {
  const flags: Values = {};
  for (const arg of cmd.flags) {
    const long = arg.long!;
    // `--dry-run` and `--yes` belong to the surface, not to the field set: one
    // is its own button, the other is supplied by the confirm step. Seeding
    // them here would put a value the form does not render into every
    // invocation.
    if (long === "dry-run" || long === "yes") continue;
    const fallback = defaultValue(arg);
    if (fieldKind(arg) === "boolean") {
      flags[long] = fallback === "true";
      continue;
    }
    // A DEFAULT IS NOT A VALUE. The manifest's default is what the CLI will do
    // if the flag is absent, so typing it into the box makes the app restate it
    // explicitly on every invocation -- `curie local deploy --api-url
    // http://localhost:28000 --api-key curie-dev-key --plugin-dir /Users/...`
    // for a command whose whole argv should have been `curie local deploy`. It
    // also overrides the CLI's own resolution with a value this app guessed,
    // which is wrong wherever that resolution is smarter than the manifest can
    // express. Defaults are the placeholder now; see `ArgInput`.
    //
    // Context IS a value. The bundle the operator is looking at, what they
    // typed last time, and what a contextual control seeded are answers to the
    // question rather than a restatement of the fallback, so they are typed in
    // and visible in the preview.
    const fromContext =
      long === "plugin-dir"
        ? (ctx.workspacePath ?? undefined)
        : STICKY_FLAGS.has(long)
          ? ctx.sticky[long]
          : undefined;
    if (fromContext) flags[long] = fromContext;
  }
  // A contextual control's own values win over everything: the operator pressed
  // "Memory" on a particular agent's row, so that agent is the answer, not the
  // one they typed into an unrelated form ten minutes ago. Unknown flags and
  // surplus positionals are dropped rather than smuggled into argv -- the form
  // renders only what the manifest declares, and the preview must stay the
  // whole truth about what will run.
  const declared = new Set(cmd.flags.map((f) => f.long!));
  for (const [long, value] of Object.entries(ctx.prefill?.flags ?? {})) {
    if (declared.has(long)) flags[long] = value;
  }
  const positionals = cmd.positionals.map((_spec, i) => ctx.prefill?.positionals?.[i] ?? "");

  return { positionals, flags };
}

/** Flags worth showing without a disclosure: the ones that carry a value the
 *  operator has already chosen, the ones with a default, and the small set that
 *  identify the target. Everything else lives under "All options" so a command
 *  with twenty flags is still readable. */
function isPrimary(arg: ManifestArg, values: Values): boolean {
  const long = arg.long!;
  if (arg.required) return true;
  if (STICKY_FLAGS.has(long)) return true;
  if (arg.default_values?.length) return true;
  const current = values[long];
  return current !== undefined && current !== "" && current !== false;
}

/**
 * Callers MUST pass `key={cmd.id}`. The form seeds itself from context once, at
 * mount, and never reseeds: a value arriving late (an API URL resolving while
 * you are mid-sentence in a message field) must not overwrite what you typed.
 * Remounting on a new command is what gives each command a clean form, and it
 * is why there is no reset effect here.
 */
export function CommandForm({
  cmd,
  onRan,
  compact,
  prefill,
}: {
  cmd: Command;
  onRan?(runId: string): void;
  compact?: boolean;
  /** Values a contextual control seeded this form with. Read once, at mount,
   *  like every other seed here. */
  prefill?: Prefill;
}) {
  const app = useApp();
  const runs = useRuns();

  const seeded = useState(() =>
    seedValues(cmd, {
      workspacePath: app.workspace?.path ?? null,
      sticky: app.sticky,
      prefill,
    }),
  )[0];
  const [positionals, setPositionals] = useState<string[]>(seeded.positionals);
  const [flags, setFlags] = useState<Values>(seeded.flags);
  const [showAll, setShowAll] = useState(false);
  const [asJson, setAsJson] = useState(false);
  const [confirming, setConfirming] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const setFlag = useCallback((long: string, value: string | boolean | undefined) => {
    setFlags((prev) => ({ ...prev, [long]: value }));
  }, []);

  // `--dry-run` and `--yes` are handled by the surface rather than as ordinary
  // fields: one is an action of its own, the other is what the confirm step
  // supplies, and rendering them as checkboxes would invite the operator to
  // pre-tick "yes, delete it".
  const dryRunFlag = cmd.flags.find((f) => f.long === "dry-run");
  const yesFlag = cmd.flags.find((f) => f.long === "yes");
  const bodyFlags = useMemo(
    () => cmd.flags.filter((f) => f.long !== "dry-run" && f.long !== "yes"),
    [cmd],
  );

  // Decided ONCE, from the values the form opened with, and then fixed for its
  // lifetime. It used to be recomputed from the live values on every render,
  // which meant using a control moved it: switching `Minimal` on made it
  // "primary", so it jumped out of the disclosure and up the form, out from
  // under the cursor that had just pressed it -- and everything below it moved
  // too. A control must not relocate because you used it.
  //
  // Seeding from the INITIAL values is still right, and is the whole point:
  // a flag a contextual control prefilled, or one the operator typed last time,
  // is something they have already chosen and belongs in view when the sheet
  // opens. The form is keyed on `cmd.id` and never reseeds, so a lazy
  // `useState` initialiser is exactly the lifetime wanted.
  const [primaryLongs] = useState(
    () => new Set(bodyFlags.filter((f) => isPrimary(f, flags)).map((f) => f.long!)),
  );
  const primary = bodyFlags.filter((f) => primaryLongs.has(f.long!));
  const secondary = bodyFlags.filter((f) => !primaryLongs.has(f.long!));

  const missing = cmd.positionals
    .map((spec, i) => (spec.required && !positionals[i]?.trim() ? spec.id : null))
    .filter(Boolean) as string[];

  // No TTY here, so these would fail on launch. The form still shows the command
  // and its arguments -- it is a reference as much as a launcher, and copying the
  // string out to a terminal is exactly the suggested path.
  const terminalOnly = !!NEEDS_TERMINAL[cmd.id];

  const preview = useMemo(
    () => renderCommand(cmd, positionals, flags, { json: asJson }),
    [cmd, positionals, flags, asJson],
  );

  const launch = useCallback(
    async (extra: Values = {}) => {
      setError(null);
      const merged = { ...flags, ...extra };
      // Remember the identifying values so the next command in this session
      // starts pre-filled. Only the sticky set, and never a secret.
      for (const [long, value] of Object.entries(merged)) {
        if (STICKY_FLAGS.has(long) && typeof value === "string" && value && long !== "api-key") {
          app.remember(long, value);
        }
      }
      try {
        const runId = await runs.start({
          action: cmd.id,
          positionals: positionals.map((p) => p.trim()),
          flags: merged,
          cwd: cwdFor(cmd, {
            workspace: app.workspace?.path,
            repoRoot: app.env?.repoRoot,
            fallback: app.env?.defaultCwd,
          }),
          json: asJson,
        });
        onRan?.(runId);
      } catch (err) {
        setError((err as Error).message);
      }
    },
    [app, runs, cmd, positionals, flags, asJson, onRan],
  );

  const run = useCallback(() => {
    if (cmd.risk === "destructive") return setConfirming(true);
    void launch();
  }, [cmd.risk, launch]);

  return (
    <div>
      {!compact ? (
        <div style={{ marginBottom: 14 }}>
          <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 5 }}>
            <Mono style={{ fontSize: 14, color: T.primary, fontWeight: 600 }}>
              curie {cmd.path.join(" ")}
            </Mono>
            <TierChip cmd={cmd} />
            {cmd.risk === "destructive" ? (
              <Badge color={STATUS.danger} filled>
                destructive
              </Badge>
            ) : null}
          </div>
          <div style={{ fontSize: 12, color: T.secondary, lineHeight: 1.55, maxWidth: M.prose }}>
            {cmd.about}
          </div>
        </div>
      ) : null}

      <Requirements cmd={cmd} />

      {cmd.positionals.map((spec, i) => (
        <Field
          key={spec.id}
          // The argument's name in words, not the CLI's usage token. `<NAME>`
          // over an empty box reads as a placeholder somebody forgot to fill in
          // -- it is the shape of a thing that is missing, which is the exact
          // wrong signal above the field you are meant to type into. The
          // mapping back to argv is not lost: it is the rendered preview under
          // the form, which is the whole truth about what will run.
          label={humanArg(spec.id)}
          hint={spec.help}
          required={spec.required}
        >
          <ArgInput
            arg={spec}
            value={positionals[i] ?? ""}
            onChange={(next) =>
              setPositionals((prev) => {
                const out = [...prev];
                out[i] = typeof next === "string" ? next : "";
                return out;
              })
            }
          />
        </Field>
      ))}

      {primary.map((arg) => (
        <FlagField key={arg.long} arg={arg} value={flags[arg.long!]} onChange={setFlag} />
      ))}

      {secondary.length ? (
        <div style={{ margin: "6px 0 14px" }}>
          <Button size="sm" tone="plain" onClick={() => setShowAll((v) => !v)}>
            {showAll ? "Hide" : `All options (${secondary.length})`}
          </Button>
          {showAll ? (
            <div
              style={{
                marginTop: 10,
                paddingLeft: 12,
                borderLeft: `1px solid ${LINE.separator}`,
              }}
            >
              {secondary.map((arg) => (
                <FlagField key={arg.long} arg={arg} value={flags[arg.long!]} onChange={setFlag} />
              ))}
            </div>
          ) : null}
        </div>
      ) : null}

      {/* The command itself, always visible. This is the contract with the
          operator: the GUI is a way to build this string, not a way around it. */}
      <div
        style={{
          background: S.well,
          border: `1px solid ${LINE.separator}`,
          borderRadius: R.control,
          padding: "9px 11px",
          display: "flex",
          alignItems: "flex-start",
          gap: 10,
          marginBottom: 12,
        }}
      >
        <Mono
          testId="command-preview"
          style={{ flex: 1, color: T.secondary, wordBreak: "break-all", lineHeight: 1.6 }}
        >
          {preview}
        </Mono>
        <CopyButton text={preview} />
      </div>

      <RunDirectory cmd={cmd} />

      {missing.length ? (
        <div style={{ marginBottom: 10 }}>
          <Notice tone="warn">
            {missing.map((m) => `<${m.toUpperCase()}>`).join(", ")} {missing.length === 1 ? "is" : "are"}{" "}
            required before this can run.
          </Notice>
        </div>
      ) : null}

      {error ? (
        <div style={{ marginBottom: 10 }}>
          <Notice tone="error" title="Could not start">
            {error}
          </Notice>
        </div>
      ) : null}

      <div style={{ display: "flex", alignItems: "center", gap: 8, flexWrap: "wrap" }}>
        <Button
          tone={cmd.risk === "destructive" ? "danger" : "primary"}
          onClick={run}
          disabled={missing.length > 0 || terminalOnly}
          title={terminalOnly ? "This command needs a real terminal" : undefined}
        >
          {cmd.risk === "destructive" ? "Review and run" : "Run"}
        </Button>

        {dryRunFlag ? (
          <Button
            onClick={() => void launch({ "dry-run": true })}
            title="Run with --dry-run: print the plan without changing anything"
          >
            Dry run
          </Button>
        ) : null}

        <div style={{ flex: 1 }} />

        {/* A platform switch, not a checkbox: a bare `<input type=checkbox>` is
            rendered by the engine and looks like a form control on a web page. */}
        <label style={{ display: "inline-flex", alignItems: "center", gap: 7 }}>
          <Toggle checked={asJson} onChange={setAsJson} />
          <Mono style={{ color: asJson ? T.secondary : T.tertiary }}>--json</Mono>
        </label>
      </div>

      {confirming ? (
        <ConfirmDestructive
          cmd={cmd}
          preview={renderCommand(cmd, positionals, yesFlag ? { ...flags, yes: true } : flags, {
            json: asJson,
          })}
          onCancel={() => setConfirming(false)}
          onConfirm={() => {
            setConfirming(false);
            // The CLI would prompt for confirmation on a TTY, and there is no
            // TTY here -- so the app's own confirm step is what supplies --yes.
            // Skipping this would hang the run on a prompt nobody can answer.
            void launch(yesFlag ? { yes: true } : {});
          }}
        />
      ) : null}
    </div>
  );
}

function TierChip({ cmd }: { cmd: Command }) {
  const color =
    cmd.tier === "skill"
      ? ACCENT
      : cmd.tier === "local"
        ? STATUS.info
        : cmd.tier === "cluster"
          ? HUE.violet
          : cmd.tier === "dev"
            ? STATUS.warn
            : T.tertiary;
  return <Badge color={color}>{cmd.tier}</Badge>;
}

/** Say up front when a command cannot work on this machine, instead of letting
 *  it fail three seconds later with a message from helm. */
function Requirements({ cmd }: { cmd: Command }) {
  const app = useApp();
  const env = app.env;

  // A command that cannot work here at all gets its own notice, and the pointer
  // to what to use instead -- not a generic "this might not work".
  const terminalOnly = NEEDS_TERMINAL[cmd.id];
  if (terminalOnly) {
    return (
      <div style={{ marginBottom: 12 }}>
        <Notice tone="warn" title="This one needs a terminal">
          {terminalOnly}
        </Notice>
      </div>
    );
  }

  if (!env) return null;

  const problems: string[] = [];
  if (!env.cliPath) problems.push("curie is not on PATH.");
  if ((cmd.tier === "skill" || cmd.tier === "local") && !env.dockerAvailable) {
    problems.push("Docker is not reachable, and this tier runs in containers.");
  }
  if (cmd.tier === "cluster" && !env.kubectlAvailable) {
    problems.push("kubectl is not on PATH.");
  }
  if (cmd.id === "cluster.up" && !env.helmAvailable) {
    problems.push("helm is not on PATH, and this command installs a Helm release.");
  }
  if (cmd.tier === "dev" && !env.sourceCheckout) {
    problems.push("This command needs a source checkout; the installed curie reports it is not one.");
  }
  if (!problems.length) return null;

  return (
    <div style={{ marginBottom: 12 }}>
      <Notice tone="warn" title="This will probably not work here">
        {problems.join(" ")}
      </Notice>
    </div>
  );
}

function FlagField({
  arg,
  value,
  onChange,
}: {
  arg: ManifestArg;
  value: string | boolean | undefined;
  onChange(long: string, value: string | boolean | undefined): void;
}) {
  const long = arg.long!;
  const kind = fieldKind(arg);

  if (kind === "boolean") {
    return (
      <div style={{ marginBottom: 10 }}>
        <Toggle
          checked={value === true || value === "true"}
          onChange={(next) => onChange(long, next)}
          // The option in words. This form IS the abstraction over the CLI, so
          // labelling its controls with the flags they compile to hands the
          // reader back the thing the form exists to save them from. The
          // mapping is not lost: the rendered preview under the form is the
          // argv, exactly.
          label={humanArg(long)}
          hint={arg.help}
        />
      </div>
    );
  }

  return (
    <Field
      label={humanArg(long)}
      hint={arg.help}
      required={arg.required}
      // No "default X" chip. The default is the placeholder now -- in the box,
      // in the position the value will occupy -- rather than a footnote beside
      // a label, where it was reported as easy to miss and was.
    >
      <ArgInput arg={arg} value={typeof value === "string" ? value : ""} onChange={(v) => onChange(long, v)} />
    </Field>
  );
}

function ArgInput({
  arg,
  value,
  onChange,
}: {
  arg: ManifestArg;
  value: string;
  onChange(next: string): void;
}) {
  const kind = fieldKind(arg);

  if (kind === "enum") {
    return (
      <Select value={value} onChange={(e) => onChange(e.target.value)}>
        <option value="">{arg.required ? "Choose…" : "(not set)"}</option>
        {(arg.possible_values ?? []).map((v) => (
          <option key={v} value={v}>
            {v}
          </option>
        ))}
      </Select>
    );
  }

  if (kind === "json") {
    return (
      <Textarea
        value={value}
        spellCheck={false}
        onChange={(e) => onChange(e.target.value)}
        placeholder={arg.default_values?.[0] ?? "{}"}
      />
    );
  }

  if (kind === "path" || kind === "file") {
    return <PathInput arg={arg} kind={kind} value={value} onChange={onChange} />;
  }

  return (
    <Input
      value={value}
      spellCheck={false}
      type={kind === "secret" ? "password" : kind === "number" ? "number" : "text"}
      autoComplete={kind === "secret" ? "off" : undefined}
      // The default goes in the box, where the value will be, rather than in a
      // chip beside the label.
      placeholder={arg.default_values?.[0] ?? ""}
      onChange={(e) => onChange(e.target.value)}
    />
  );
}

/**
 * A path: choose it, drop it, or type it.
 *
 * It was a text box, so the only way to supply a compose file or a plugin
 * directory was to know its absolute path and type it correctly -- the CLI's
 * own ergonomics reproduced in a window that has a file dialog sitting right
 * there. Typing still works and is still the field's own state; the button and
 * the drop target are two more ways in, not a replacement.
 *
 * The drop target is the field, not a separate zone: a zone that appears only
 * while dragging cannot be discovered, and one that is always there costs
 * height on every form for a gesture most people will not use.
 */
function PathInput({
  arg,
  kind,
  value,
  onChange,
}: {
  arg: ManifestArg;
  kind: "path" | "file";
  value: string;
  onChange(next: string): void;
}) {
  const app = useApp();
  const [over, setOver] = useState(false);

  const choose = async () => {
    const picked = await bridge().dialog.pick({
      kind: kind === "file" ? "file" : "directory",
      title: `Choose ${humanArg(arg.long ?? arg.id).toLowerCase()}`,
    });
    if (picked) onChange(picked);
  };

  return (
    <div
      onDragOver={(e) => {
        e.preventDefault();
        setOver(true);
      }}
      onDragLeave={() => setOver(false)}
      onDrop={(e) => {
        e.preventDefault();
        setOver(false);
        const file = e.dataTransfer.files[0];
        // Electron removed `File.path` in 32; the preload's `webUtils` shim is
        // the only way back to a real path, and it returns null rather than
        // guessing when there is not one.
        const path = file ? bridge().dialog.pathForFile(file) : null;
        if (path) onChange(path);
      }}
      style={{
        display: "flex",
        gap: 7,
        alignItems: "center",
        borderRadius: R.control,
        outline: over ? `2px solid ${ACCENT}` : "2px solid transparent",
        outlineOffset: 2,
        transition: "outline-color 120ms ease",
      }}
    >
      <Input
        value={value}
        spellCheck={false}
        // The default in the box. Where there is none, say what shape of thing
        // belongs here rather than leaving it blank.
        placeholder={
          over
            ? "Drop it here"
            : (arg.default_values?.[0] ??
              runtimeDefault(arg, app.env) ??
              (kind === "file" ? "Choose a file, or drop one here" : "Choose a directory"))
        }
        onChange={(e) => onChange(e.target.value)}
        style={{ fontFamily: FONT.mono, flex: 1, minWidth: 0 }}
      />
      <Button size="sm" onClick={() => void choose()} style={{ flex: "none" }}>
        Choose…
      </Button>
    </div>
  );
}

function ConfirmDestructive({
  cmd,
  preview,
  onCancel,
  onConfirm,
}: {
  cmd: Command;
  preview: string;
  onCancel(): void;
  onConfirm(): void;
}) {
  const [typed, setTyped] = useState("");
  // The confirmation word is the command's own leaf name, so muscle memory
  // cannot carry an operator through a teardown they did not mean to run.
  const word = cmd.name;
  return (
    <Sheet
      title={`${commandTitle(cmd.id, cmd.path)}?`}
      onClose={onCancel}
      footer={
        <>
          <Button onClick={onCancel}>Cancel</Button>
          <Button tone="danger" disabled={typed.trim() !== word} onClick={onConfirm}>
            Run it
          </Button>
        </>
      }
    >
      <Notice tone="error" title="This changes or removes live state">
        {cmd.about}
      </Notice>
      <div
        style={{
          margin: "14px 0",
          background: S.well,
          border: `1px solid ${LINE.separator}`,
          borderRadius: R.control,
          padding: "9px 11px",
        }}
      >
        <Mono style={{ color: T.secondary, wordBreak: "break-all" }}>{preview}</Mono>
      </div>
      <Field label={<>Type <Mono style={{ color: T.primary }}>{word}</Mono> to confirm</>}>
        <Input value={typed} autoFocus onChange={(e) => setTyped(e.target.value)} spellCheck={false} />
      </Field>
    </Sheet>
  );
}
