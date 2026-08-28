// "New agent": pick a starting point, name it, make it.
//
// This replaced a button that ran the scaffolder and handed back an empty
// directory -- which answers "how do I make one" with a blank page and a file
// tree. Every builder worth using opens on a set of starting points instead,
// because the question somebody actually arrives with is not "what is an agent
// made of" but "which of these is closest to what I want".
//
// The copy here is the standard for the rest of this app: it is read by
// somebody deciding what to build, not by somebody operating a platform. No
// container, no tier, no bundle, no command name. What the agent DOES, and a
// sample exchange, which is faster to judge than any description.

import { useState } from "react";

import { useApp } from "../bridge/app";
import { bridge } from "../bridge/bridge";
import { TEMPLATES, type Template } from "../lib/templates";
import { ACCENT, F, LINE, R, S, T, tint } from "../tokens";
import { Button, Field, Group, Input, Mono, Notice, Sheet } from "../primitives";

/** A name the scaffolder will accept, derived from what was typed. Offered as
 *  the field's starting value so nobody meets a validation error they could
 *  have been handed the answer to. */
function slug(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
}

export function NewAgent({ onClose }: { readonly onClose: () => void }) {
  const app = useApp();
  const [picked, setPicked] = useState<Template>(TEMPLATES[0]);
  const [name, setName] = useState("");
  const [where, setWhere] = useState(app.env?.defaultCwd ?? "");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const id = slug(name);
  const ready = !!id && !!where && !busy;

  const create = async () => {
    setBusy(true);
    setError(null);
    const res = await app.createAgent({ parentDir: where, name: id, files: picked.files(id) });
    setBusy(false);
    if (res.ok) onClose();
    else setError(res.error);
  };

  return (
    <Sheet
      title="New agent"
      onClose={onClose}
      width={720}
      footer={
        <div style={{ display: "flex", gap: 8, justifyContent: "flex-end", width: "100%" }}>
          <Button tone="plain" onClick={onClose}>
            Cancel
          </Button>
          <Button tone="primary" busy={busy} disabled={!ready} onClick={() => void create()}>
            Create agent
          </Button>
        </div>
      }
    >
      <div style={{ display: "grid", gap: 16 }}>
        <div style={{ display: "grid", gap: 8 }}>
          <div style={{ ...F.section, color: T.tertiary }}>Start from</div>
          <div style={{ display: "grid", gap: 8 }}>
            {TEMPLATES.map((t) => (
              <TemplateCard
                key={t.id}
                template={t}
                picked={t.id === picked.id}
                onPick={() => setPicked(t)}
              />
            ))}
          </div>
        </div>

        {picked.example.length ? (
          <div style={{ display: "grid", gap: 8 }}>
            <div style={{ ...F.section, color: T.tertiary }}>What it looks like</div>
            <Group style={{ padding: 12, display: "grid", gap: 6 }}>
              {picked.example.map((line, i) => (
                <div key={i} style={{ display: "flex", gap: 10, alignItems: "baseline" }}>
                  <span
                    style={{
                      ...F.footnote,
                      color: line.from === "you" ? T.quaternary : ACCENT,
                      width: 44,
                      flex: "none",
                      textAlign: "right",
                    }}
                  >
                    {line.from}
                  </span>
                  <span style={{ ...F.body, color: T.secondary }}>{line.text}</span>
                </div>
              ))}
            </Group>
          </div>
        ) : null}

        <Field
          label="Name"
          hint={
            id && id !== name.trim()
              ? `It will be called ${id}.`
              : "What you will call it. Lower case, hyphens instead of spaces."
          }
        >
          <Input
            value={name}
            autoFocus
            spellCheck={false}
            placeholder="shift-notes"
            onChange={(e) => setName(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && ready) void create();
            }}
          />
        </Field>

        <Field label="Where to keep it" hint="A new folder is made here.">
          <div style={{ display: "flex", gap: 7, alignItems: "center" }}>
            <Input
              value={where}
              spellCheck={false}
              onChange={(e) => setWhere(e.target.value)}
              style={{ flex: 1, minWidth: 0, fontFamily: "var(--font-mono)" }}
            />
            <Button
              size="sm"
              style={{ flex: "none" }}
              onClick={() => {
                void bridge()
                  .dialog.pick({ kind: "directory", title: "Where to keep it" })
                  .then((p) => p && setWhere(p));
              }}
            >
              Choose…
            </Button>
          </div>
        </Field>

        {id && where ? (
          <div style={{ ...F.footnote, color: T.quaternary }}>
            Creates <Mono style={{ fontSize: 11 }}>{`${where}/${id}`}</Mono>
          </div>
        ) : null}

        {error ? (
          <Notice tone="error" title="Could not create it">
            {error}
          </Notice>
        ) : null}
      </div>
    </Sheet>
  );
}

function TemplateCard({
  template,
  picked,
  onPick,
}: {
  readonly template: Template;
  readonly picked: boolean;
  readonly onPick: () => void;
}) {
  return (
    <button
      onClick={onPick}
      aria-pressed={picked}
      style={{
        display: "block",
        width: "100%",
        textAlign: "left",
        padding: "11px 13px",
        borderRadius: R.group,
        border: `1px solid ${picked ? ACCENT : LINE.separator}`,
        background: picked ? tint(ACCENT, 0.08) : S.control,
        cursor: "default",
        color: "inherit",
      }}
    >
      <div style={{ ...F.headline, color: T.primary }}>{template.name}</div>
      <div style={{ ...F.callout, color: T.secondary, marginTop: 2, lineHeight: 1.5 }}>
        {template.tagline}
      </div>
      {picked ? (
        <div style={{ ...F.footnote, color: T.tertiary, marginTop: 6, lineHeight: 1.55 }}>
          {template.about}
        </div>
      ) : null}
    </button>
  );
}
