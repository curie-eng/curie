// Configuring an agent with controls, rather than by opening its files.
//
// Everything here was already configurable -- it just meant knowing that the
// description lives in `plugin.json`, that what the agent should do is the prose
// under a YAML frontmatter block in `skills/<name>/SKILL.md`, and that the
// suggestions it offers are a JSON array called `starterPrompts`. That is the
// CLI's filing system, and knowing it is the price this window exists to remove.
//
// It edits the same files the editor below does, through `bundle.ts`'s pure
// write functions, so there is one definition of what each file means and both
// surfaces stay in agreement. It refuses to write a file it cannot parse rather
// than replacing it with what it could model: half of somebody's agent is not
// this panel's to discard.

import { useEffect, useState } from "react";

import { bridge, type Workspace } from "../bridge/bridge";
import {
  skillBody,
  withPluginField,
  withSkillBody,
  type PluginManifest,
} from "../lib/bundle";
import { F, LINE, R, S, T } from "../tokens";
import { Button, Field, Group, Input, Notice, SectionHeader, Textarea } from "../primitives";

/** A field's own save state. Per field, not per panel: one failing write must
 *  not make the others look unsaved. */
type State = "clean" | "dirty" | "saving" | "saved";

export function AgentSettings({
  ws,
  plugin,
  onSaved,
}: {
  readonly ws: Workspace;
  readonly plugin: PluginManifest | undefined;
  readonly onSaved: () => void;
}) {
  // The first skill is the agent's own. A bundle can hold several, and the file
  // list below is where those are edited; this panel is for the common case of
  // one agent doing one thing.
  const skillPath = ws.skills.length ? `skills/${ws.skills[0]}/SKILL.md` : null;
  const [skillText, setSkillText] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    if (!skillPath) return;
    void bridge()
      .workspace.readFile(ws.path, skillPath)
      .then((t) => {
        if (!cancelled) setSkillText(t);
      })
      .catch(() => {
        if (!cancelled) setSkillText(null);
      });
    return () => {
      cancelled = true;
    };
  }, [ws.path, skillPath]);

  const writeManifest = async (key: string, value: string | readonly string[] | undefined) => {
    const current = await bridge().workspace.readFile(ws.path, ".claude-plugin/plugin.json");
    const next = withPluginField(current, key, value);
    if (!next.ok) {
      setError(next.error);
      return false;
    }
    await bridge().workspace.writeFile(ws.path, ".claude-plugin/plugin.json", next.value);
    setError(null);
    onSaved();
    return true;
  };

  return (
    <section>
      <SectionHeader>Settings</SectionHeader>
      <Group style={{ padding: 14, display: "grid", gap: 16 }}>
        {error ? (
          <Notice tone="error" title="Not saved">
            {error}
          </Notice>
        ) : null}

        {skillPath && skillText !== null ? (
          <SavedTextarea
            label="What it should do"
            hint="Written to the agent every time it runs. Be specific: this is the whole of what it knows about its job."
            rows={10}
            value={skillBody(skillText)}
            onSave={async (body) => {
              const next = withSkillBody(skillText, body);
              await bridge().workspace.writeFile(ws.path, skillPath, next);
              setSkillText(next);
              onSaved();
              return true;
            }}
          />
        ) : (
          <div style={{ ...F.footnote, color: T.tertiary }}>
            This agent has nothing telling it what to do yet. Add a skill and it will appear here.
          </div>
        )}

        <SavedInput
          label="Description"
          hint="One line, shown wherever this agent is listed."
          value={plugin?.description ?? ""}
          onSave={(v) => writeManifest("description", v)}
        />

        <ListField
          label="Things to suggest"
          hint="Offered to somebody who has not used this agent before and does not know what to ask."
          values={plugin?.starterPrompts ?? []}
          placeholder="What can you help me with?"
          onSave={(v) => writeManifest("starterPrompts", v)}
        />

        {/* Said plainly rather than left to be discovered. The dials that only
            exist once an agent is running are genuinely elsewhere, and an
            operator hunting this panel for them should be told where they are
            rather than concluding they do not exist. */}
        <div
          style={{
            ...F.footnote,
            color: T.tertiary,
            lineHeight: 1.55,
            borderTop: `1px solid ${LINE.separator}`,
            paddingTop: 12,
          }}
        >
          Which model it uses, how hard it thinks, where it answers and what it may spend are set
          per running agent, not here — those live on the agent&apos;s own row on the Overview once
          you have put it to work. Anything else about this one is in its files below.
        </div>
      </Group>
    </section>
  );
}

/** A field that saves on blur and says so, rather than one that needs a button
 *  found and pressed for every change. */
function SavedInput({
  label,
  hint,
  value,
  onSave,
}: {
  readonly label: string;
  readonly hint: string;
  readonly value: string;
  readonly onSave: (v: string) => Promise<boolean>;
}) {
  const [draft, setDraft] = useState(value);
  const [state, setState] = useState<State>("clean");

  // The file is the truth. If it changed underneath -- the editor below, another
  // program -- take the new value, unless there is an unsaved edit to lose.
  const [seen, setSeen] = useState(value);
  if (value !== seen && state === "clean") {
    setSeen(value);
    setDraft(value);
  }

  const commit = async () => {
    if (draft === value) return setState("clean");
    setState("saving");
    setState((await onSave(draft)) ? "saved" : "dirty");
  };

  return (
    <Field label={label} hint={hint} right={<Saved state={state} />}>
      <Input
        value={draft}
        onChange={(e) => {
          setDraft(e.target.value);
          setState("dirty");
        }}
        onBlur={() => void commit()}
        onKeyDown={(e) => {
          if (e.key === "Enter") (e.target as HTMLInputElement).blur();
        }}
      />
    </Field>
  );
}

function SavedTextarea({
  label,
  hint,
  value,
  rows,
  onSave,
}: {
  readonly label: string;
  readonly hint: string;
  readonly value: string;
  readonly rows: number;
  readonly onSave: (v: string) => Promise<boolean>;
}) {
  const [draft, setDraft] = useState(value);
  const [state, setState] = useState<State>("clean");
  const [seen, setSeen] = useState(value);
  if (value !== seen && state === "clean") {
    setSeen(value);
    setDraft(value);
  }

  const commit = async () => {
    if (draft === value) return setState("clean");
    setState("saving");
    setState((await onSave(draft)) ? "saved" : "dirty");
  };

  return (
    <Field label={label} hint={hint} right={<Saved state={state} />}>
      <Textarea
        value={draft}
        rows={rows}
        spellCheck
        onChange={(e) => {
          setDraft(e.target.value);
          setState("dirty");
        }}
        onBlur={() => void commit()}
      />
    </Field>
  );
}

/** A list of short strings: add, edit, remove. Saved on blur like the rest. */
function ListField({
  label,
  hint,
  values,
  placeholder,
  onSave,
}: {
  readonly label: string;
  readonly hint: string;
  readonly values: readonly string[];
  readonly placeholder: string;
  readonly onSave: (v: readonly string[]) => Promise<boolean>;
}) {
  const [draft, setDraft] = useState<string[]>([...values]);
  const [state, setState] = useState<State>("clean");
  const [seen, setSeen] = useState(values);
  if (values !== seen && state === "clean") {
    setSeen(values);
    setDraft([...values]);
  }

  const commit = async (next: string[]) => {
    const cleaned = next.map((v) => v.trim()).filter(Boolean);
    setState("saving");
    setState((await onSave(cleaned)) ? "saved" : "dirty");
  };

  return (
    <Field label={label} hint={hint} right={<Saved state={state} />}>
      <div style={{ display: "grid", gap: 6 }}>
        {draft.map((v, i) => (
          <div key={i} style={{ display: "flex", gap: 6, alignItems: "center" }}>
            <Input
              value={v}
              placeholder={placeholder}
              onChange={(e) => {
                const next = [...draft];
                next[i] = e.target.value;
                setDraft(next);
                setState("dirty");
              }}
              onBlur={() => state === "dirty" && void commit(draft)}
              style={{ flex: 1, minWidth: 0 }}
            />
            <Button
              size="sm"
              tone="plain"
              aria-label={`Remove suggestion ${i + 1}`}
              onClick={() => {
                const next = draft.filter((_, j) => j !== i);
                setDraft(next);
                void commit(next);
              }}
              style={{ flex: "none" }}
            >
              Remove
            </Button>
          </div>
        ))}
        <div>
          <Button
            size="sm"
            onClick={() => {
              setDraft([...draft, ""]);
              setState("dirty");
            }}
          >
            Add one
          </Button>
        </div>
      </div>
    </Field>
  );
}

/** Whether this particular field is saved. Per field, because one failing write
 *  must not make every other field look unsaved. */
function Saved({ state }: { readonly state: State }) {
  if (state === "clean") return null;
  const text =
    state === "saving" ? "saving…" : state === "saved" ? "saved" : "unsaved";
  return (
    <span
      style={{
        ...F.footnote,
        color: state === "dirty" ? T.tertiary : T.quaternary,
        background: state === "dirty" ? S.control : "transparent",
        borderRadius: R.pill,
        padding: state === "dirty" ? "1px 7px" : 0,
      }}
    >
      {text}
    </span>
  );
}
