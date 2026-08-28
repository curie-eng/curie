// Making an agent, as a sequence of decisions rather than one wall.
//
// This was a single sheet holding everything at once: pick a starting point,
// name it, choose a folder, and a Create button, all live from the moment it
// opened. That shape asks somebody to hold four unrelated questions in their
// head simultaneously, and it puts the irreversible control on screen before the
// first decision has been made.
//
// Three steps, because there are three actual decisions: WHICH kind of agent,
// WHAT to call it and where it goes, and THEN a look at what is about to be
// written before anything is. The last one earns its place -- it is the answer
// to "what am I going to get", which a scaffolder that just makes a directory
// has never given anybody.
//
// Copy rule, as everywhere: read by somebody deciding what to build, not
// operating a platform. No container, no bundle, no command.

import { useState } from "react";

import { useApp } from "../bridge/app";
import { bridge } from "../bridge/bridge";
import { TEMPLATES, type Template } from "../lib/templates";
import { ACCENT, F, LINE, R, S, T, tint } from "../tokens";
import { Button, Field, Group, Input, Mono, Notice, Sheet } from "../primitives";

/**
 * Tall enough that NO step scrolls, and identical for all of them.
 *
 * Measured, not guessed: the tallest is the first step showing the shared-list
 * template, at 468px of content. This box carries the sheet's own 18px bottom
 * inset inside its height (`box-sizing: border-box` is global), so it has to be
 * that much larger again; 532 leaves a little over for a font that renders
 * slightly bigger than the one measured on.
 *
 * The `min` is the short-window case. `Sheet` caps itself at 84vh, so on a small
 * display a fixed 480 would be taller than the panel could ever be and the body
 * would be clipped rather than scrolled -- content unreachable with no scrollbar
 * to say so. 168 is the sheet's own header and footer, measured at 162 and
 * rounded up; below that height the body shrinks and scrolls, which is the
 * honest outcome when there is genuinely not enough room.
 */
const BODY_HEIGHT = "min(532px, calc(84vh - 168px))";

type StepId = "start" | "name" | "review";

const STEPS: readonly { id: StepId; label: string }[] = [
  { id: "start", label: "Start from" },
  { id: "name", label: "Name it" },
  { id: "review", label: "Review" },
];

/** A name the scaffolder will accept, derived from what was typed. Offered as
 *  the field's own answer so nobody meets a validation error they could have
 *  been handed the fix for. */
function slug(text: string): string {
  return text
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "")
    .slice(0, 40);
}

export function NewAgent({ onClose }: { readonly onClose: () => void }) {
  const app = useApp();
  const [step, setStep] = useState<StepId>("start");
  const [picked, setPicked] = useState<Template>(TEMPLATES[0]);
  const [name, setName] = useState("");
  const [where, setWhere] = useState(app.env?.defaultCwd ?? "");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const id = slug(name);
  const index = STEPS.findIndex((s) => s.id === step);

  // Each step names what it is waiting for, so the Next button can say why it is
  // disabled instead of just being grey.
  const blocked =
    step === "name" && !id
      ? "Give it a name first."
      : step === "name" && !where
        ? "Say where to keep it."
        : null;

  const create = async () => {
    setBusy(true);
    setError(null);
    const res = await app.createAgent({ parentDir: where, name: id, files: picked.files(id) });
    setBusy(false);
    if (res.ok) onClose();
    else setError(res.error);
  };

  const back = () => {
    setError(null);
    setStep(STEPS[Math.max(0, index - 1)].id);
  };
  const next = () => {
    setError(null);
    setStep(STEPS[Math.min(STEPS.length - 1, index + 1)].id);
  };

  return (
    <Sheet
      title="New agent"
      onClose={onClose}
      width={720}
      // Fixed, so no step resizes the panel. Passed to the sheet rather than
      // applied to a wrapper here: a second scrolling box would clip at its own
      // padding edge and cut the shadow off every card inside it.
      bodyHeight={BODY_HEIGHT}
      footer={
        <div style={{ display: "flex", alignItems: "center", gap: 10, width: "100%" }}>
          <span style={{ ...F.footnote, color: T.quaternary }}>
            Step {index + 1} of {STEPS.length}
          </span>
          {blocked ? (
            <span style={{ ...F.footnote, color: T.tertiary }}>{blocked}</span>
          ) : null}
          <span style={{ flex: 1 }} />
          <Button tone="plain" onClick={index === 0 ? onClose : back}>
            {index === 0 ? "Cancel" : "Back"}
          </Button>
          {step === "review" ? (
            <Button tone="primary" busy={busy} onClick={() => void create()}>
              Create agent
            </Button>
          ) : (
            <Button tone="primary" disabled={!!blocked} onClick={next}>
              Next
            </Button>
          )}
        </div>
      }
    >
      <Steps current={index} />

      {step === "start" ? (
        <StartFrom picked={picked} onPick={setPicked} />
      ) : step === "name" ? (
        <NameIt
          name={name}
          onName={setName}
          slugged={id}
          where={where}
          onWhere={setWhere}
          onSubmit={() => !blocked && next()}
        />
      ) : (
        <Review template={picked} name={id} where={where} />
      )}

      {error ? (
        <div style={{ marginTop: 14 }}>
          <Notice tone="error" title="Could not create it">
            {error}
          </Notice>
        </div>
      ) : null}
    </Sheet>
  );
}

/** Where you are and how much is left. Steps behind you are marked done rather
 *  than merely un-highlighted: "you have answered this" and "you have not got
 *  there yet" are different states and a single accent colour conflates them. */
function Steps({ current }: { readonly current: number }) {
  return (
    <div style={{ display: "flex", alignItems: "center", gap: 8, marginBottom: 18 }}>
      {STEPS.map((s, i) => {
        const done = i < current;
        const on = i === current;
        return (
          <span key={s.id} style={{ display: "inline-flex", alignItems: "center", gap: 8 }}>
            {i > 0 ? (
              <span
                aria-hidden
                style={{ width: 18, height: 1, background: LINE.separator, flex: "none" }}
              />
            ) : null}
            <span
              style={{
                display: "inline-flex",
                alignItems: "center",
                gap: 6,
                padding: "3px 10px",
                borderRadius: R.pill,
                background: on ? tint(ACCENT, 0.16) : done ? S.control : "transparent",
                ...F.caption,
                color: on ? T.primary : done ? T.secondary : T.quaternary,
              }}
            >
              <span aria-hidden style={{ ...F.footnote, color: on ? ACCENT : T.quaternary }}>
                {done ? "✓" : i + 1}
              </span>
              {s.label}
            </span>
          </span>
        );
      })}
    </div>
  );
}

function StartFrom({
  picked,
  onPick,
}: {
  readonly picked: Template;
  readonly onPick: (t: Template) => void;
}) {
  return (
    <div style={{ display: "grid", gap: 16 }}>
      {/* Every card the same height, whichever is selected. The description used
          to appear inside the chosen one, so picking a card resized it and
          shoved the two below it down the page -- selection is not supposed to
          move the thing you are selecting between. */}
      <div style={{ display: "grid", gap: 8 }}>
        {TEMPLATES.map((t) => (
          <TemplateCard
            key={t.id}
            template={t}
            picked={t.id === picked.id}
            onPick={() => onPick(t)}
          />
        ))}
      </div>

      <div style={{ display: "grid", gap: 8 }}>
        <div style={{ ...F.section, color: T.tertiary }}>
          {picked.example.length ? "What it looks like" : "About this one"}
        </div>
        <Group style={{ padding: 12, display: "grid", gap: 8 }}>
          <div style={{ ...F.footnote, color: T.tertiary, lineHeight: 1.55 }}>{picked.about}</div>
          {picked.example.length ? (
            <div style={{ display: "grid", gap: 6, marginTop: 2 }}>
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
            </div>
          ) : null}
        </Group>
      </div>
    </div>
  );
}

function NameIt({
  name,
  onName,
  slugged,
  where,
  onWhere,
  onSubmit,
}: {
  readonly name: string;
  readonly onName: (v: string) => void;
  readonly slugged: string;
  readonly where: string;
  readonly onWhere: (v: string) => void;
  readonly onSubmit: () => void;
}) {
  return (
    <div style={{ display: "grid", gap: 16 }}>
      <Field
        label="Name"
        hint={
          slugged && slugged !== name.trim()
            ? `It will be called ${slugged}.`
            : "What you will call it. Lower case, hyphens instead of spaces."
        }
      >
        <Input
          value={name}
          autoFocus
          spellCheck={false}
          placeholder="shift-notes"
          onChange={(e) => onName(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") onSubmit();
          }}
        />
      </Field>

      <Field label="Where to keep it" hint="A new folder is made here.">
        <div style={{ display: "flex", gap: 7, alignItems: "center" }}>
          <Input
            value={where}
            spellCheck={false}
            onChange={(e) => onWhere(e.target.value)}
            style={{ flex: 1, minWidth: 0, fontFamily: "var(--font-mono)" }}
          />
          <Button
            size="sm"
            style={{ flex: "none" }}
            onClick={() => {
              void bridge()
                .dialog.pick({ kind: "directory", title: "Where to keep it" })
                .then((p) => p && onWhere(p));
            }}
          >
            Choose…
          </Button>
        </div>
      </Field>

      {/* The answer to both fields at once, while they are still being typed.
          The review step says it again, and that is not a duplicate: here it is
          live feedback on what you are typing, there it is the last thing you
          read before anything is written. */}
      {slugged && where ? (
        <Group style={{ padding: "10px 12px" }}>
          <div style={{ ...F.footnote, color: T.tertiary }}>
            This makes{" "}
            <Mono style={{ fontSize: 11, color: T.primary }}>{`${where}/${slugged}`}</Mono>
          </div>
        </Group>
      ) : null}
    </div>
  );
}

/**
 * What is about to be written, before it is.
 *
 * The file list is the point. A scaffolder that just makes a directory leaves
 * somebody to discover its shape afterwards, one file at a time; naming them
 * here turns "what am I going to get" into something answered before the
 * irreversible step rather than after it.
 */
function Review({
  template,
  name,
  where,
}: {
  readonly template: Template;
  readonly name: string;
  readonly where: string;
}) {
  const overlay = Object.keys(template.files(name));
  return (
    <div style={{ display: "grid", gap: 14 }}>
      <Group style={{ padding: 14, display: "grid", gap: 10 }}>
        <Row label="Starting from" value={template.name} />
        <Row label="Called" value={name} mono />
        <Row label="Folder" value={`${where}/${name}`} mono />
      </Group>

      <div style={{ display: "grid", gap: 8 }}>
        <div style={{ ...F.section, color: T.tertiary }}>What gets written</div>
        <Group style={{ padding: 14 }}>
          <div style={{ ...F.footnote, color: T.tertiary, lineHeight: 1.6, marginBottom: 8 }}>
            The usual pieces every agent has — what it should do, the examples that check it, and
            the settings that say where it can be used
            {overlay.length ? ", with these written for you:" : "."}
          </div>
          {overlay.length ? (
            <div style={{ display: "grid", gap: 3 }}>
              {overlay.map((f) => (
                <Mono key={f} style={{ fontSize: 11, color: T.secondary }}>
                  {f}
                </Mono>
              ))}
            </div>
          ) : null}
        </Group>
      </div>

      <div style={{ ...F.footnote, color: T.quaternary, lineHeight: 1.55 }}>
        Nothing is written until you press Create. You can change any of it afterwards.
      </div>
    </div>
  );
}

function Row({
  label,
  value,
  mono,
}: {
  readonly label: string;
  readonly value: string;
  readonly mono?: boolean;
}) {
  return (
    <div style={{ display: "flex", gap: 12, alignItems: "baseline" }}>
      <span style={{ ...F.footnote, color: T.quaternary, width: 96, flex: "none" }}>{label}</span>
      {mono ? (
        <Mono style={{ fontSize: 12, color: T.primary }}>{value}</Mono>
      ) : (
        <span style={{ ...F.body, color: T.primary }}>{value}</span>
      )}
    </div>
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
    </button>
  );
}
